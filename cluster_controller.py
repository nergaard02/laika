from fastapi import FastAPI, HTTPException
from models import TaskState, make_task_id
from raft_types import LogEntry
from state_machine import SchedulerStateMachine
import os, time, asyncio, random, json, httpx, socket

NODE_ID = os.environ.get("MY_ADDRESS")
CLUSTER_NODES = os.environ.get("CLUSTER_NODES")
STATE_FILE = f"{NODE_ID.replace(':','_')}_raft_state.json"
NODE_TIMEOUT = 20

if not NODE_ID or not CLUSTER_NODES:
    sys.exit(1)

CLUSTER_NODES = CLUSTER_NODES.split(",")

PEERS = [node for node in CLUSTER_NODES if node != NODE_ID]

nodes = {}

app = FastAPI()

TASK_TTL = 60
MAX_RUNTIME = 3600 # 1 hour
HEARTBEAT_INTERVAL = 0.3
ELECTION_TIMEOUT_MIN = 8.0
ELECTION_TIMEOUT_MAX = 9.0

class ClusterController:
    def __init__(self, peers, my_address):
        self.peers = peers # list of other controller addresses
        self.my_address = my_address
        self.leader_address = None
        
        self.role = "follower"
        self.election_timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX) # election timeouts are chosen randomly from a fixed interval
        self.last_heartbeat = time.time() # Last time since receiving AppendEntries RPC from current leader or granting vote to candidate
        self.heartbeat_interval = HEARTBEAT_INTERVAL # must be < election timeout
        
        # Persistent state on all servers:
        # (Updated on stable storage before responding to RPCs)
        self.current_term = 0
        self.voted_for = None
        self.log = []
        
        # Volatile state on all servers:
        self.commit_index = -1  # index of highest log entry known to be committed
        self.last_applied = -1  # index of highest log entry applied to state machine
        
        # Volatile state on leaders:
        self.next_index = {} # for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
        self.match_index = {} # for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
        
        self.state_machine = SchedulerStateMachine()
        self.load_state()
        
        self.replication_locks = {peer: asyncio.Lock() for peer in peers}
    
    async def initialize_tasks(self):
        # Only run if no tasks exist yet
        if self.log:
            return
        
        await load_tasks_from_dataset()
        
    def persist_term(self):
        append_record({
            "type": "term",
            "value": self.current_term
        })
    
    def persist_vote(self):
        append_record({
            "type": "vote",
            "value": self.voted_for
        })
    
    def persist_log_entry(self, entry):
        append_record({
            "type": "log",
            "entry": entry.dict()
        })

    def load_state(self):
        if not os.path.exists(STATE_FILE):
            return

        with open(STATE_FILE, "r") as f:
            for line in f:
                line = line.strip()

                if not line:
                    continue

                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if data["type"] == "term":
                    self.current_term = data["value"]

                elif data["type"] == "vote":
                    self.voted_for = data["value"]

                elif data["type"] == "log":
                    self.log.append(LogEntry(**data["entry"]))
    
    async def election_loop(self):
        while True:
            await asyncio.sleep(0.1)
            
            if self.role == "leader":
                continue

            if time.time() - self.last_heartbeat > self.election_timeout:
                await self.start_election()
    
    async def heartbeat_loop(self):
        while self.role == "leader":
            await asyncio.gather(
                *(self.replicate_to_peer(peer) for peer in self.peers)
            )
            
            await asyncio.sleep(self.heartbeat_interval)
    
    def last_log_index(self):
        return len(self.log) - 1

    def last_log_term(self):
        if not self.log:
            return 0
        return self.log[-1].term
        
    async def propose(self, entry: LogEntry):
        if self.role != "leader":
            return False
        
        # Append locally
        self.log.append(entry)
        self.persist_log_entry(entry)
        
        index = self.last_log_index()
        
        majority = (len(self.peers) + 1) // 2 + 1
        
        while self.role == "leader":
            await asyncio.gather(*(self.replicate_to_peer(peer) for peer in self.peers))
            
            success_count = 1 # leader counts itself
            
            # count based on match index
            for peer in self.peers:
                if self.match_index.get(peer, -1) >= index:
                    success_count += 1
            
            # Check if we have replicated the entry to a majority and can quit
            if success_count >= majority and index > self.commit_index:
                # Only committ entries from current term
                if self.log[index].term == self.current_term:
                    self.commit_index = index
                    self.apply_committed_entries()
                    return True
            
            await asyncio.sleep(0.05)
        
        # Did not manage to replicate on a majority of followers
        return False
        
    async def start_election(self):
        self.role = "candidate"
        
        self.current_term += 1
        self.persist_term()
        
        self.voted_for = self.my_address
        self.persist_vote()
        
        self.last_heartbeat = time.time()
        self.election_timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        
        votes = 1 + await self.send_request_votes()
        
        
        # If we stepped down during vote collection
        if self.role != "candidate":
            return
        
        majority = (len(self.peers) + 1) // 2 + 1
        
        if votes >= majority:
            self.role = "leader"
            
            self.leader_address = self.my_address
            
            for peer in self.peers:
                self.next_index[peer] = len(self.log)
                self.match_index[peer] = -1
            
            asyncio.create_task(self.initialize_tasks())
            asyncio.create_task(self.heartbeat_loop())
            asyncio.create_task(rescheduler_loop())
    
    async def send_request_votes(self):
        votes = 0
        async with httpx.AsyncClient() as client:
            for peer in self.peers:
                try:
                    response = await client.post(
                        f"http://{peer}/request_vote",
                        json={
                            "term": self.current_term,
                            "candidate_id": self.my_address,
                            "last_log_index": self.last_log_index(), # index of candidate's last log entry
                            "last_log_term": self.last_log_term(),  # term of candidate's last log entry
                        },
                        timeout=2.0,
                    )
                    
                    data = response.json()
                    
                    # If RPC request or response contains term T > currentTerm: set currentTerm = T, and convert to follower (§5.1)
                    if data["term"] > self.current_term:
                        self.current_term = data["term"]
                        self.persist_term()
                        
                        self.role = "follower"
                        
                        self.voted_for = None
                        self.persist_vote()
                        return 0

                    if data["vote_granted"]:
                        votes += 1
                except Exception:
                    continue
            return votes
    
    def apply_committed_entries(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            self.state_machine.apply(entry)
        
    
    async def replicate_to_peer(self, peer):
        if self.role != "leader":
            return False
        
        async with self.replication_locks[peer]:
        
            next_idx = self.next_index[peer]
            prev_idx = next_idx - 1
            
            if prev_idx >= 0:
                prev_term = self.log[prev_idx].term
            else:
                prev_term = 0
                
            entries = self.log[next_idx:]
            
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        f"http://{peer}/append_entries",
                        json={
                            "term": self.current_term,
                            "leader_id": self.my_address,
                            "prev_log_index": prev_idx,     # Here, the paper says that it should send the prev index from its own log, not the follower, look at this
                            "prev_log_term": prev_term,
                            "entries": [e.dict() for e in entries],
                            "leader_commit": self.commit_index,
                        },
                        timeout=2.0,
                    )
                        
                        
                    data = response.json()
                        
                    if data["term"] > self.current_term:
                        self.current_term = data["term"]
                        self.persist_term()
                        
                        self.role = "follower"
                        
                        self.voted_for = None
                        self.persist_vote()
                        return False
                        
                    # Update follower progress
                    if data["success"]:
                        self.match_index[peer] = prev_idx + len(entries)
                        self.next_index[peer] = self.match_index[peer] + 1
                        return True
                    
                    # Follower log mismatch, decrement nextIndex
                    else:
                        self.next_index[peer] = max(0, self.next_index[peer] - 1)
                    
            except Exception:
                pass
            
            await asyncio.sleep(0.05)
            return False
        

controller = ClusterController(PEERS, NODE_ID)

async def load_tasks_from_dataset():
    root = "/share/inf3203/unlabeled_images"
    for name in os.listdir(root):
        path = os.path.join(root, name) # create the total path by concatenating the root path with the name of the image
        task_id = make_task_id(path)

        entry = LogEntry(
            term = controller.current_term,
            command = "CREATE_TASK",
            task_id = task_id,
            image_path=path
        )
        
        await controller.propose(entry)
        
async def shutdown_after_timeout():
    await asyncio.sleep(MAX_RUNTIME)
    os._exit(0)

@app.on_event("startup")
async def startup():
    asyncio.create_task(shutdown_after_timeout())
    asyncio.create_task(controller.election_loop())
    asyncio.create_task(monitor_loop())

    
def check_leader():
    if controller.role != "leader":
        return {
            "error": "not_leader",
            "leader": controller.leader_address
        }
    
    return None
    

@app.post("/request_task")
async def request_task(node_id: str):
    # Check if this cluster controller is the raft leader, redirect to leader if not
    redirect = check_leader()
    if redirect:
        return redirect
    
    # find a pending task
    pending_task = None
    for task in controller.state_machine.tasks.values():
        if task.state == TaskState.PENDING:
            pending_task = task
            break
    
    # could not find any pending tasks
    if not pending_task:
        return None
    
    entry = LogEntry(
        term = controller.current_term,
        command = "ASSIGN_TASK",
        task_id = pending_task.id,
        node_id = node_id,
        lease_expiry = time.time() + TASK_TTL
    )
    
    # propose state changes
    committed = await controller.propose(entry)
    
    if not committed:
        return None
    
    task = controller.state_machine.tasks[pending_task.id]
    
    # If apply() rejects the assignment, return None
    if task.assigned_node != node_id:
        return None
    
    # return updated task
    return task

@app.post("/task_done")
async def task_done(task_id: str, node_id: str, label: str):
    redirect = check_leader()
    if redirect:
        return redirect
    
    task = controller.state_machine.tasks.get(task_id)
    
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    entry = LogEntry(
        term = controller.current_term,
        command = "COMPLETE_TASK",
        task_id = task_id,
        node_id = node_id,
        label = label
    )
    
    committed = await controller.propose(entry)
    
    if not committed:
        return {"status": "rejected"}
    
    return {"status": "ok"}

@app.get("/status")
def status():
    counts = {
        state.value: sum(1 for t in controller.state_machine.tasks.values() if t.state == state)
        for state in TaskState
    }

    return {
        "leader": controller.leader_address,
        "role": controller.role,
        
        # raft internals
        "current_term": controller.current_term,
        "commit_index": controller.commit_index,
        "committed_terms": [
            entry.term for entry in controller.log[: controller.commit_index + 1]
        ],
        
        # last few log entries (including uncommitted)
        "log_tail": [
            entry.term for entry in controller.log[-5:]
        ],
        "log_length": len(controller.log),
        
        # scheduler state
        "task_counts": counts,
        "node_load": controller.state_machine.node_load,
        "nodes": controller.state_machine.nodes
    }
    
@app.post("/request_vote")
async def request_vote(data: dict):
    if controller.role == "leader":
        return {
            "term": controller.current_term,
            "vote_granted": False
        }
        
    term = data["term"]
    candidate_id = data["candidate_id"]
    last_log_index = data["last_log_index"]
    last_log_term = data["last_log_term"]
    
    
    # Reject stale term
    if term < controller.current_term:
        return {
            "term": controller.current_term,
            "vote_granted": False,
        }
    
    # If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
    if term > controller.current_term:
        controller.current_term = term
        controller.persist_term()
        
        controller.role = "follower"
        
        controller.voted_for = None
        controller.persist_vote()
    
    # Check if candidate's log is at least as up to date as your's
    my_last_log_index = controller.last_log_index()
    my_last_log_term = controller.last_log_term()
    
    log_up_to_date = (
        last_log_term > my_last_log_term or
        (last_log_term == my_last_log_term and last_log_index >= my_last_log_index)
    )
    
    if log_up_to_date and (
        controller.voted_for is None or
        controller.voted_for == candidate_id
    ):
        controller.voted_for = candidate_id
        controller.persist_vote()
        
        # Reset election timer since we heard from a candidate
        controller.last_heartbeat = time.time()
        controller.election_timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        
        
        return {
            "term": controller.current_term,
            "vote_granted": True,
        }

    
    return {
        "term": controller.current_term,
        "vote_granted": False,
    }
        
        
@app.post("/append_entries")
async def append_entries(data: dict):
    term = data["term"]
    
    if term < controller.current_term:
        return {
            "term": controller.current_term,
            "success": False,
        }
    
    if term > controller.current_term:
        controller.current_term = term
        controller.persist_term()
        controller.voted_for = None
        controller.persist_vote()
    
    controller.role = "follower"
    controller.leader_address = data["leader_id"]
    controller.last_heartbeat = time.time()
    controller.election_timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
    
    
    prev_log_index = data["prev_log_index"]
    prev_log_term = data["prev_log_term"]
    
    # Ensure log consistency between leader and follower at the point where new entries will be appended
    # 2: Reply false if log does not contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
    if prev_log_index >= 0:
        if prev_log_index >= len(controller.log):
            return {
                "term": controller.current_term,
                "success": False,
            }
            
        if controller.log[prev_log_index].term != prev_log_term:
            return {
                "term": controller.current_term,
                "success": False,
            }
    
    entries = data["entries"]
    index = prev_log_index + 1
    i = 0
    
    
    # 3: If an existing entry conflicts with a new one (same index but different terms),
    # delete the existing entry and all that follow it (§5.3)
    while i < len(entries):
        if index < len(controller.log):
            if controller.log[index].term != entries[i]["term"]:
                controller.log = controller.log[:index]
                break
        
        else:
            break
        index += 1
        i += 1
    
    # 4: Append any new entries not already in the log
    while i < len(entries):
        controller.log.append(LogEntry(**entries[i]))
        controller.persist_log_entry(LogEntry(**entries[i]))
        i += 1

    # 5: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
    leader_commit = data["leader_commit"]
    
    if leader_commit > controller.commit_index:
        controller.commit_index = min(
            leader_commit,
            len(controller.log) - 1,
        )
        controller.apply_committed_entries()
    
    return {
        "term": controller.current_term,
        "success": True,
    }

async def rescheduler_loop():
    while True:
        if controller.role != "leader":
            await asyncio.sleep(1)
            continue
        
        now = time.time()
        
        for task in list(controller.state_machine.tasks.values()):
            # task has expired, reschedule it
            if task.state == TaskState.ASSIGNED and task.lease_expiry and task.lease_expiry < now: 
                
                entry = LogEntry(
                    term = controller.current_term,
                    command = "RESCHEDULE_TASK",
                    task_id = task.id
                )
                
                await controller.propose(entry)

        await asyncio.sleep(5)

async def monitor_loop():
    while True:
        if controller.role != "leader":
            await asyncio.sleep(2)
            continue

        now = time.time()
        
        for node_id, node in controller.state_machine.nodes.items():
            # Do not need to check nodes that are already stated as dead
            if not node["alive"]:
                continue
            
            healthy = await check_node_health(node_id, node)
            
            if healthy:
                continue
            
            node["alive"] = False
            
            entry = LogEntry(
                term = controller.current_term,
                command = "NODE_FAILED",
                node_id = node_id,
            )
            
            await controller.propose(entry)
            
            await activate_replica()
        
        await asyncio.sleep(3)
        
        
async def check_node_health(node_id, node):
    try:
        port = node["port"]
        
        async with httpx.AsyncClient(timeout=2.0) as client:
            response = await client.get(f"http://{node_id}:{port}/health")
            
        if response.status_code != 200:
            return False
        
        data = response.json()
        
        workers = data.get("workers")
        
        if workers is not None:
            node["workers"] = workers
        
        return True

    except Exception:
        return False
    
async def activate_replica():
    for node_id, node in controller.state_machine.nodes.items():
        
        # Only activate alive nodes
        if not node["alive"]:
            continue
        
        # Only activate replicas
        if node["workers"] != 0:
            continue
        
        port = node["port"]
        
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                response = await client.post(
                    f"http://{node_id}:{port}/activate_workers",
                    json={"workers": 4} # numbers of workers to start
                )
            
            if response.status_code == 200:
                
                # update state locally
                node["workers"] = 4
                
                return True
        
        except Exception:
            continue
    
    return False
    
        
def append_record(record):
    with open(STATE_FILE, "a") as f:
        json.dump(record, f)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
        

@app.post("/register_node")
async def register_node(node_id: str, workers: int, port: int):
    redirect = check_leader()
    if redirect:
        return redirect
    
    entry = LogEntry(
        term=controller.current_term,
        command="REGISTER_NODE",
        node_id=node_id,
        workers=workers,
        port=port,
    )
    
    committed = await controller.propose(entry)
    
    if not committed:
        return {"status": "failed"}
    
    
    return {"status": "ok"}

    
                
                    
    
        