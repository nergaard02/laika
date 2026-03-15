
import os, sys, httpx, subprocess, socket, asyncio, uvicorn, threading
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

MAX_RUNTIME = 3600 # 1 hour

if len(sys.argv) < 4:
    sys.exit(1)

AGENT_COUNT = int(sys.argv[1])
CLUSTER_NODES = sys.argv[2].split(",")
PORT = int(sys.argv[3])

NODE_ID = socket.gethostname()

FETCH_INTERVAL = 2      # seconds between fetch attempts
LOCAL_QUEUE_LIMIT = 5   # max tasks buffered locally

current_leader = None

LOCAL_QUEUE = asyncio.Queue(maxsize=LOCAL_QUEUE_LIMIT)

WORKERS = {
    i: {
        "process": None,
        "busy": False,
        "task": None,
    }
    for i in range(AGENT_COUNT)
}

class ActivateWorkersRequest(BaseModel):
    workers: int

async def register_with_cluster():
    while True:
        data = await contact_cluster(
            "register_node",
            {
                "node_id": NODE_ID,
                "workers": AGENT_COUNT,
                "port": PORT
            }
        )
        
        if data and data.get("status") == "ok":
            return
        
        await asyncio.sleep(2)

@app.get("/health")
def health():
    return {
        "node": NODE_ID,
        "workers": AGENT_COUNT,
        "status": "ok"
    }

@app.post("/activate_workers")
async def activate_workers(req: ActivateWorkersRequest):
    global AGENT_COUNT, WORKERS
    
    workers = req.workers
    
    if AGENT_COUNT > 0:
        return {"status": "already active"}
    
    AGENT_COUNT = workers
    
    
    WORKERS = {
        i: {
            "process": None,
            "busy": False,
            "task": None,
        }
        for i in range(workers)
    }
    
    asyncio.create_task(fetch_loop())
    asyncio.create_task(dispatch_loop())
    asyncio.create_task(monitor_loop())
    
    return {"status": "activated"}
    

## Helper
async def contact_cluster(endpoint, params):
    global current_leader
    
    
    async with httpx.AsyncClient(timeout=5.0) as client:
        
        nodes_to_contact = []
        
        if current_leader:
            nodes_to_contact.append(current_leader)
        
        for node in CLUSTER_NODES:
            if node not in nodes_to_contact:
                nodes_to_contact.append(node)
        
        for node in nodes_to_contact:
            try: 
                response = await client.post(
                    f"http://{node}/{endpoint}",
                    params=params,
                )
            
                if response.status_code != 200:
                    continue
                
                data = response.json()

                if isinstance(data, dict) and data.get("error") == "not_leader":
                    current_leader = data.get("leader")
                    continue
                
                current_leader = node
                return data
            
            except httpx.RequestError:
                continue
        
        return None

# Fetch tasks
async def fetch_loop():
    # Replica, no workers
    if AGENT_COUNT == 0:
        return
    
    while True:
        if LOCAL_QUEUE.qsize() < LOCAL_QUEUE_LIMIT and find_idle_worker() is not None:
            data = await contact_cluster(
                "request_task",
                {"node_id": NODE_ID}
            )
            
            if data and "id" in data:
                await LOCAL_QUEUE.put(data)

        await asyncio.sleep(FETCH_INTERVAL)

def find_idle_worker():
    for worker_id, worker in WORKERS.items():
        if not worker["busy"]:
            return worker_id
    
    return None

def start_worker(worker_id, task):
    process = subprocess.Popen(
        ["venv/bin/python", "agent.py", task["image_path"]],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    WORKERS[worker_id]["process"] = process
    WORKERS[worker_id]["busy"] = True
    WORKERS[worker_id]["task"] = task

async def dispatch_loop():
    if AGENT_COUNT == 0:
        return
    
    while True:
        task = await LOCAL_QUEUE.get()
        worker_id = find_idle_worker()
        
        if worker_id is None:
            await LOCAL_QUEUE.put(task)
            await asyncio.sleep(1)
            continue
        
        start_worker(worker_id, task)
    

async def monitor_loop():
    if AGENT_COUNT == 0:
        return
    
    while True:
        for worker_id, worker in WORKERS.items():
            if worker["busy"]:
                process = worker["process"]
                
                if process.poll() is not None:
                    task = worker["task"]
                    
                    # The worker finished successfully
                    if process.returncode == 0:
                        output = process.stdout.read().strip()
                        label = output
                        
                        await contact_cluster(
                            "task_done",
                            {
                                "task_id": task["id"],
                                "node_id": NODE_ID,
                                "label": label,
                            }
                        )
                    
                    # Free worker slot, worker could have either finished or crashed, either way, reset worker slow
                    WORKERS[worker_id]["process"] = None
                    WORKERS[worker_id]["busy"] = False
                    WORKERS[worker_id]["task"] = None
            
        await asyncio.sleep(1)
    
async def shutdown_after_timeout():
    await asyncio.sleep(MAX_RUNTIME)
    os._exit(0) # Hard exit, will also kills child processes too (workers)
    
async def main():
    
    asyncio.create_task(shutdown_after_timeout())
    
    await register_with_cluster()
    
    if AGENT_COUNT > 0:
        asyncio.create_task(fetch_loop())
        asyncio.create_task(dispatch_loop())
        asyncio.create_task(monitor_loop())
    
    while True:
        await asyncio.sleep(60)

def start_server():
    uvicorn.run(app, host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    server_thread = threading.Thread(target=start_server, daemon=True)
    server_thread.start()
    
    asyncio.run(main())
                        
                        
                    
                
    