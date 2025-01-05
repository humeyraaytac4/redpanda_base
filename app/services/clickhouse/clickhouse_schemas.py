from pydantic import BaseModel
from typing import Optional

class DatabaseRequest(BaseModel):
    database_name: str


class QueryRequest(BaseModel):
    query: str


class ConsumeRequest(BaseModel):
    topic: str
    timeout: Optional[float] = 1.0
    max_messages: Optional[int] = 10