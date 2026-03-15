from state_machine import SchedulerStateMachine
from raft_types import LogEntry
from models import TaskState
import subprocess, time, requests, json, os

PROJECT_DIR = os.getcwd()


RUN_SCRIPT = "./run.sh"
CLEAN_SCRIPT = "./cleanup.sh"

TEST_RUNTIME = 120 # seconds

# Helpers
def start_cluster():
    print("Starting cluster...\n")
    result = subprocess.run(["bash", RUN_SCRIPT])
    print("Started cluster\n")
    time.sleep(15)
    
def stop_cluster():
    subprocess.run(["bash", CLEAN_SCRIPT])
    
def extract_replica_address(output):
    lines = output.splitlines()
    start = lines.index("Replica nodes started:")
    replica_line = lines[start + 1].strip()
    node, port = replica_line.split(":")
    return node, port

def extract_worker_nodes(output):
    lines = output.splitlines()
    start = lines.index("Worker nodes started:")
    local_controllers = []
    i = start + 1
    while i < len(lines) and ":" in lines[i]:
        local_controllers.append(lines[i].strip())
        i += 1
    
    return local_controllers

def load_cluster_info():
    with open("cluster_info.json") as f:
        return json.load(f)

def find_leader(cluster_controllers):
    for address in cluster_controllers:
        node, port = address.split(":")
        try:
            response = requests.get(f"http://{node}:{port}/status", timeout=2)
            data = response.json()
            
            if data["leader"]:
                print(f"I {address} have my leader as: {data['leader']}")
                return data["leader"]
        
        except:
            continue
    
    return None

def find_follower(cluster_controllers):
    leader = find_leader(cluster_controllers)
    
    for address in cluster_controllers:
        if address != leader:
            return address
    
    return None

# The actual tests
def test_duplicate_task():
    sm = SchedulerStateMachine()
    
    sm.apply(LogEntry(term=1, command="CREATE_TASK", task_id="t1", image_path="img"))
    
    # First assign to node 1
    sm.apply(LogEntry(term=1, command="ASSIGN_TASK", task_id="t1", node_id="n1", lease_expiry=10))
    
    # Lease expired, reschedule to node 2
    sm.apply(LogEntry(term=1, command="RESCHEDULE_TASK", task_id="t1"))
    sm.apply(LogEntry(term=1, command="ASSIGN_TASK", task_id="t1", node_id="n2", lease_expiry=10))
    
    sm.apply(LogEntry(term=1, command="COMPLETE_TASK", task_id="t1", node_id="n2", label="dog"))
    
    # Node 1 later recovers and tries to finish the task
    sm.apply(LogEntry(term=1, command="COMPLETE_TASK", task_id="t1", node_id="n1", label="cat"))
    
    task = sm.tasks["t1"]
    
    # Make sure that the task is still assigned to node 2 and that the result from node 1 was ignored
    assert task.assigned_node == "n2"
    assert task.label == "dog"
    assert task.state == TaskState.DONE

def test_replica_activation():
    start_cluster()
    
    cluster_info = load_cluster_info()
    
    replica_node, replica_port = cluster_info["replicas"][0].split(":")
    
    # Select the local controller to be killed, the first one
    local_controller_node, local_controller_port = cluster_info["local_controllers"][0].split(":")
    
    print(f"Replica node: {replica_node}, replica_port: {replica_port}\n")
    
    try:
        response = requests.get(f"http://{replica_node}:{replica_port}/health")
        data = response.json()
        
        # Make sure the replica actually has 0 workers
        assert data["workers"] == 0
        
        print(f"Killing local controller on node {local_controller_node}")
        
        # Simulate a crash
        subprocess.run([
            "ssh",
            local_controller_node,
            "pkill -f local_controller.py",
        ])
        
        # Let the system have time to detect the crash to activate the replica
        time.sleep(20)
        
        response = requests.get(f"http://{replica_node}:{replica_port}/health")
        data = response.json()
        
        assert data["workers"] > 0
    
    finally:
        stop_cluster()


def test_node_rejoin_after_crash():
    start_cluster()
    
    cluster_info = load_cluster_info()
    
    # Select the local controller to kill
    local_controller_node, local_controller_port = cluster_info["local_controllers"][0].split(":")
    
    # The cluster controller we will check against
    cluster_controller_node, cluster_controller_port = cluster_info["cluster_controllers"][0].split(":")
    
    try:
        # Verify that the local controller has registered with the cluster
        response = requests.get(f"http://{cluster_controller_node}:{cluster_controller_port}/status")
        
        data = response.json()
        
        matching_node = None
        
        for node in data["nodes"]:
            if node.startswith(local_controller_node):
                matching_node = node
                break
        
        assert matching_node is not None
        assert data["nodes"][matching_node]["alive"] is True
        
        print(f"Node {local_controller_node} initially registered")
        
        # Kill the local controller
        subprocess.run([
            "ssh",
            local_controller_node,
            "pkill -f local_controller.py",
        ])
        
        print(f"Killed local controller on {local_controller_node}")
        
        # Let the cluster controllers register the failure
        time.sleep(15)
        
        response = requests.get(f"http://{cluster_controller_node}:{cluster_controller_port}/status")
        data = response.json()
        
        assert data["nodes"][matching_node]["alive"] is False
        
        print(f"Node {local_controller_node} marked as dead")
        
        cluster_string = ",".join(cluster_info["cluster_controllers"])
        print("Restart cluster string:", cluster_string)

        restart_cmd = (
            f"cd {PROJECT_DIR} && "
            f"nohup venv/bin/python -u local_controller.py "
            f"2 {cluster_string} {local_controller_port} "
            f"> $(hostname)_local_restarted.log 2>&1"
        )

        subprocess.run([
            "ssh",
            "-n",
            "-f",
            local_controller_node,
            restart_cmd
        ])
        
        print(f"Restarted local controller on {local_controller_node}")
        
        # Wait for local controller to register again
        time.sleep(5)
        
        response = requests.get(f"http://{cluster_controller_node}:{cluster_controller_port}/status")
        data = response.json()
        
        assert data["nodes"][matching_node]["alive"] is True
        
        print(f"Node {local_controller_node} successfully rejoined the cluster")
        
    finally:
        stop_cluster()
        

# Raft implementation tests
def test_leader_election_after_crash():
    start_cluster()
    
    cluster_info = load_cluster_info()
    cluster_controllers = cluster_info["cluster_controllers"]
    
    try:
        # Wait for cluster to stabilize (choose the first leader)
        time.sleep(5)
        
        leader = find_leader(cluster_controllers)
        
        assert leader is not None
        print(f"Initial leader: {leader}")
        
        leader_node, leader_port = leader.split(":")
        
        # Simulate a crash of the leader 
        subprocess.run([
            "ssh",
            leader_node,
            f"fuser -k {leader_port}/tcp"
        ])
        
        print(f"Killed leader:{leader_node}")
        
        # Wait for a new raft node to be elected as leader
        time.sleep(10)
        
        new_leader = find_leader(cluster_controllers)
        
        print(f"New leader:{new_leader}")
        
        assert new_leader is not None
        assert new_leader != leader
    
    finally:
        stop_cluster()

def test_old_leader_steps_down_after_restart():
    start_cluster()
    
    cluster_info = load_cluster_info()
    cluster_controllers = cluster_info["cluster_controllers"]
    
    try:
        time.sleep(12)
        
        # Find current leader
        leader = find_leader(cluster_controllers)
        assert leader is not None
        
        print(f"Initial leader: {leader}")
        
        leader_node, leader_port = leader.split(":")
        
        # Kill the leader
        subprocess.run([
            "ssh",
            leader_node,
            f"fuser -k {leader_port}/tcp"
        ])
        
        print(f"Leader killed: {leader}")
        
        # Wait for new election
        time.sleep(12)
        
        new_leader = find_leader(cluster_controllers)
        
        assert new_leader is not None
        assert new_leader != leader
        
        print(f"New leader: {new_leader}")
        
        # Restart the old leader
        restart_cmd = (
            f"cd {PROJECT_DIR} && "
            f"MY_ADDRESS={leader_node}:{leader_port} "
            f"CLUSTER_NODES={','.join(cluster_controllers)} "
            f"nohup venv/bin/python -m uvicorn cluster_controller:app "
            f"--host 0.0.0.0 --port {leader_port} "
            f"> $(hostname)_cluster_restarted.log 2>&1"
        )
        
        
        subprocess.run(["ssh", "-n", "-f", leader_node, restart_cmd])
        
        print("Old leader restarted")
        
        # Let the old leader register that it no longer is the leader
        time.sleep(5)
        
        # Query the restarted node
        response = requests.get(f"http://{leader_node}:{leader_port}/status")
        data = response.json()
        
        print(f"Restarted node role: {data['role']}")
        
        # Make sure that the leader stepped down as leader
        assert data["role"] != "leader"
    
    finally:
        stop_cluster()

def test_follower_log_catchup_after_restart():
    start_cluster()
    
    cluster_info = load_cluster_info()
    cluster_controllers = cluster_info["cluster_controllers"]
    
    try:
        # Let a leader be selected
        time.sleep(12)
        
        leader = find_leader(cluster_controllers)
        assert leader is not None
        
        follower = find_follower(cluster_controllers)
        assert follower is not None
        
        print(f"Leader: {leader}")
        print(f"Follower to kill: {follower}")
        
        follower_node, follower_port = follower.split(":")
        leader_node, leader_port = leader.split(":")
        
        leader_status = requests.get(f"http://{leader_node}:{leader_port}/status").json()
        leader_commit_index_before = leader_status["commit_index"]
        
        # Kill follower
        subprocess.run([
            "ssh",
            follower_node,
            f"fuser -k {follower_port}/tcp"
        ])
        
        print("Follower killed")
        
        # Let the cluster continue processing tasks
        time.sleep(10)
        
        # Restart follower
        restart_cmd = (
            f"cd {PROJECT_DIR} && "
            f"MY_ADDRESS={follower_node}:{follower_port} "
            f"CLUSTER_NODES={','.join(cluster_controllers)} "
            f"nohup venv/bin/python -m uvicorn cluster_controller:app "
            f"--host 0.0.0.0 --port {follower_port} "
            f"> $(hostname)_cluster_restarted.log 2>&1"
        )
        
        subprocess.run(["ssh", "-n", "-f", follower_node, restart_cmd])
        
        print("Follower restarted")
        
        # Let the restarted follower catch up
        time.sleep(10)
        
        leader_status = requests.get(f"http://{leader_node}:{leader_port}/status").json()
        follower_status = requests.get(f"http://{follower_node}:{follower_port}/status").json()
        
        print(f"Follower commit index: {follower_status['commit_index']}")
        
        follower_commit_index = follower_status["commit_index"]
        
        leader_committed_entries = leader_status["committed_terms"]
        follower_committed_entries = follower_status["committed_terms"]
        
        # Make sure that the follower acrually catched up after the crash
        assert follower_commit_index >= leader_commit_index_before
        
        # Check the log matching property:
        # If two logs contain an entry with the same index and term, then the logs are identical up through that entry
        assert leader_committed_entries[: follower_commit_index + 1] == follower_committed_entries[: follower_commit_index + 1]
    
    finally:
        stop_cluster()

def test_log_conflict_resolution():
    start_cluster()
    
    cluster_info = load_cluster_info()
    cluster_controllers = cluster_info["cluster_controllers"]
    
    try:
        time.sleep(12)
        
        # Find leader1
        leader1 = find_leader(cluster_controllers)
        assert leader1 is not None
        
        print(f"Leader1: {leader1}")
        
        leader1_node, leader1_port = leader1.split(":")
        
        # Kill the followers to isolate leader1
        followers = [n for n in cluster_controllers if n != leader1]
        
        for node in followers:
            node_name, node_port = node.split(":")
            subprocess.run([
                "ssh",
                node_name,
                f"fuser -k {node_port}/tcp"
            ])
            
        print("Followers killed")
        
        # Let leader1 generate uncomitted entries
        time.sleep(10)
        
        leader1_status = requests.get(f"http://{leader1_node}:{leader1_port}/status").json()
        
        old_commit_index = leader1_status["commit_index"]
        old_log_length = leader1_status["log_length"]
        
        # index of last entry in the log of leader1
        old_last_index = old_log_length - 1
        
        # term of that entry
        old_last_term = leader1_status["log_tail"][-1]
        print(f"Old last entry: {old_last_index}, {old_last_term}")
        
        print(f"Leader1 commit index: {old_commit_index}")
        print(f"Leader1 log length: {old_log_length}")
        
        # Verify leader1 created uncommited entries
        assert old_commit_index < old_log_length - 1
        
        # Kill leader1
        subprocess.run([
            "ssh",
            leader1_node,
            f"fuser -k {leader1_port}/tcp"
        ])
        
        print("Leader1 killed")
        
        time.sleep(10)
        
        # Restart followers so they elect new leader
        for node in followers:
            node_name, port = node.split(":")
            
            restart_cmd = (
                f"cd {PROJECT_DIR} && "
                f"MY_ADDRESS={node_name}:{port} "
                f"CLUSTER_NODES={','.join(cluster_controllers)} "
                f"nohup venv/bin/python -m uvicorn cluster_controller:app "
                f"--host 0.0.0.0 --port {port} "
                f"> $(hostname)_restarted.log 2>&1"
            )
            
            subprocess.run(["ssh", "-n", "-f", node_name, restart_cmd])
        
        print("Followers restarted")
        
        # Wait for a new leader to be elected
        time.sleep(80)
        
        leader2 = find_leader(cluster_controllers)
        
        assert leader2 is not None
        assert leader2 != leader1
        
        print(f"Leader2: {leader2}")
        
        leader2_node, leader2_port = leader2.split(":")
        
        # Allow leader 2 to create entries
        time.sleep(10)
        
        # Restart leader1
        restart_cmd = (
            f"cd {PROJECT_DIR} && "
            f"MY_ADDRESS={leader1_node}:{leader1_port} "
            f"CLUSTER_NODES={','.join(cluster_controllers)} "
            f"nohup venv/bin/python -m uvicorn cluster_controller:app "
            f"--host 0.0.0.0 --port {leader1_port} "
            f"> $(hostname)_restarted.log 2>&1"
        )
        
        subprocess.run(["ssh", "-n", "-f", leader1_node, restart_cmd])
        
        print("Leader1 restarted")
        
        # Allow log conflicts to be resolved
        time.sleep(10)
        
        leader2_status = requests.get(f"http://{leader2_node}:{leader2_port}/status").json()
        leader1_status = requests.get(f"http://{leader1_node}:{leader1_port}/status").json()
        
        leader2_commit_index = leader2_status["commit_index"]
        leader1_commit_index = leader1_status["commit_index"]
        
        leader2_committed_entries = leader2_status["committed_terms"]
        leader1_committed_entries = leader1_status["committed_terms"]
        
        # Check that the log entries that were not committed by leader1 was deleted
        if old_last_index <= leader1_commit_index:
            assert leader1_committed_entries[old_last_index] != old_last_term
        
        follower_commit_index = min(leader1_commit_index, leader2_commit_index)
        
        # Check that the logs are now identical up to the commit index
        assert leader1_committed_entries[: follower_commit_index + 1] == leader2_committed_entries[: follower_commit_index + 1]
        print("Log conflict resolved correctly")
    
    finally:
        stop_cluster()

def test_outdated_log_cannot_win_election():
    start_cluster()
    
    cluster_info = load_cluster_info()
    cluster_controllers = cluster_info["cluster_controllers"]
    
    try:
        time.sleep(12)
        
        leader = find_leader(cluster_controllers)
        assert leader is not None
        print(f"Leader: {leader}")
        
        # choose follower to fall behind
        outdated_node = find_follower(cluster_controllers)
        assert outdated_node is not None
        print(f"Outdated node: {outdated_node}")
        
        outdated_node_name, outdated_node_port = outdated_node.split(":")
        leader_node, leader_port = leader.split(":")
        
        # Kill outdated follower so it falls behind
        subprocess.run([
            "ssh",
            outdated_node_name,
            f"fuser -k {outdated_node_port}/tcp"
        ])
        
        print("Outdated follower killed")
        
        # Let leader commit more entries
        time.sleep(10)
        
        # Kill leader
        subprocess.run([
            "ssh",
            leader_node,
            f"fuser -k {leader_port}/tcp"
        ])
        
        print("Leader killed")
        
        # Restart outdated follower
        restart_cmd = (
            f"cd {PROJECT_DIR} && "
            f"MY_ADDRESS={outdated_node_name}:{outdated_node_port} "
            f"CLUSTER_NODES={','.join(cluster_controllers)} "
            f"nohup venv/bin/python -m uvicorn cluster_controller:app "
            f"--host 0.0.0.0 --port {outdated_node_port} "
            f"> $(hostname)_restarted.log 2>&1"
        )
        
        subprocess.run(["ssh", "-n", "-f", outdated_node_name, restart_cmd])
        
        print("Outdated follower restarted")
        
        # Wait for election
        time.sleep(12)
        
        new_leader = find_leader(cluster_controllers)
        
        print(f"New leader: {new_leader}")
        
        # The outdated node should not have won the election
        assert new_leader != outdated_node
        
        print("Outdated node correctly rejected as leader")
    
    finally:
        stop_cluster()
        
        
        
            
    
