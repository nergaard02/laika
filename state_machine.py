from models import Task, TaskState
import os, time

LABEL_ROOT = "labeled-data"

class SchedulerStateMachine:
    def __init__(self):
        self.tasks = {}
        self.node_load = {}
        self.nodes = {} # list of known local controllers
        
        os.makedirs(LABEL_ROOT, exist_ok=True)
    
    def apply(self, entry):
        command = entry.command
        
        if command == "CREATE_TASK":
            # idempotency protection
            if entry.task_id in self.tasks:
                return
        
            task = Task(id=entry.task_id, image_path=entry.image_path)
            self.tasks[entry.task_id] = task
        
        elif command == "ASSIGN_TASK":
            task = self.tasks.get(entry.task_id)
            
            # task must exist
            if not task:
                return
            
            # only assign if still pending
            if task.state != TaskState.PENDING:
                return
            
            task.state = TaskState.ASSIGNED
            task.assigned_node = entry.node_id
            task.lease_expiry = entry.lease_expiry
            
            # If this node has never been seen before, register it and say it currently has 0 tasks
            self.node_load.setdefault(entry.node_id, 0)
            self.node_load[entry.node_id] += 1
        
        elif command == "COMPLETE_TASK":
            task = self.tasks.get(entry.task_id)
            
            if not task:
                return 

            # already completed
            if task.state == TaskState.DONE:
                return

            if task.state != TaskState.ASSIGNED:
                return
            
            if task.assigned_node != entry.node_id:
                return

            task.state = TaskState.DONE
            task.label = entry.label
            
            if task.assigned_node:
                self.node_load[task.assigned_node] -= 1
            
            label_dir = os.path.join(
                LABEL_ROOT,
                entry.label.replace(" ", "_")
            )
            
            os.makedirs(label_dir, exist_ok=True)
            
            image_name = os.path.basename(task.image_path)
            link_path = os.path.join(label_dir, image_name)
            
            # Create symlink if it does not exist
            if not os.path.exists(link_path):
                os.symlink(task.image_path, link_path)
        
        elif command == "RESCHEDULE_TASK":
            task = self.tasks.get(entry.task_id)
            
            if not task:
                return

            # onnly reschedule if still assigned
            if task.state != TaskState.ASSIGNED:
                return

            # decrement load before clearing the assignment
            if task.assigned_node:
                self.node_load[task.assigned_node] -= 1
            
            task.state = TaskState.PENDING
            task.assigned_node = None
            task.lease_expiry = None
        
        elif command == "REGISTER_NODE":
            # Do not register the same node multiple times
            if entry.node_id in self.nodes:
                # Node up and running after crash
                node = self.nodes[entry.node_id]
                node["alive"] = True
                node["workers"] = entry.workers
                node["port"] = entry.port
                node["last_seen"] = time.time()
                return
            
            # New node
            self.nodes[entry.node_id] = {
                "workers": entry.workers,
                "is_replica": entry.workers == 0,
                "alive": True,
                "last_seen": time.time(),
                "port": entry.port,
            }
        
        elif command == "NODE_FAILED":
            node = self.nodes.get(entry.node_id)
            
            if not node:
                return
            
            if not node["alive"]:
                return
            
            print(f"Node failed: {entry.node_id}")
            
            node["alive"] = False
            
            
            
        
        
            