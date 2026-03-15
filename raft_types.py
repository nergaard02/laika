from pydantic import BaseModel
from typing import Optional

class LogEntry(BaseModel):
    term: int
    command: str
    
    task_id: Optional[str] = None
    image_path: Optional[str] = None
    node_id: Optional[str] = None
    lease_expiry: Optional[float] = None
    label: Optional[str] = None
    
    workers: Optional[int] = None
    port: Optional[int] = None
    