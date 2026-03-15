from enum import Enum
from pydantic import BaseModel
from typing import Optional
import time
import hashlib

class TaskState(str, Enum):
    PENDING = "PENDING"
    ASSIGNED = "ASSIGNED"
    DONE = "DONE"
    
    
def make_task_id(path: str) -> str:
    return hashlib.sha1(path.encode()).hexdigest()

class Task(BaseModel):
    id: str
    image_path: str
    state: TaskState = TaskState.PENDING
    assigned_node: Optional[str] = None
    lease_expiry: Optional[float] = None
    label: Optional[str] = None
    