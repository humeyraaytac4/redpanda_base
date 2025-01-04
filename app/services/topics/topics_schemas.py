from pydantic import BaseModel
from typing import List

# Topic oluşturma isteği için model
class CreateTopicRequest(BaseModel):
    name: str
    partitions: int = 1
    replication_factor: int = 1

# Mesaj üretme isteği için model
class ProduceMessageRequest(BaseModel):
    topic: str
    messages: List[dict]
    interval: float = 1.0  # Varsayılan olarak 1 saniye aralık

# Mesaj tüketme isteği için model
class ConsumeMessagesRequest(BaseModel):
    topic: str
    max_messages: int = 10  # Varsayılan olarak 10 mesaj
    timeout: float = 1.0  # Varsayılan olarak 1 saniye zaman aşımı