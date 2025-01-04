from fastapi import APIRouter, Depends, HTTPException, Query
from app.services.topics.topics_schemas import CreateTopicRequest
from app.services.topics.topics import KafkaService
import os
from app.services.json_datas.json_data_generator import generate_random_messages
from typing import Optional
from fastapi import BackgroundTasks

# Kafka broker adresini çevre değişkeninden al
broker = os.getenv("KAFKA_BROKER", "redpanda:29092")

# KafkaService'ı başlat
kafka_service = KafkaService(broker)

topics_router = APIRouter()

# KafkaService bağımlılığını doğrudan bir fonksiyonla sağlama
def get_kafka_service() -> KafkaService:
    return kafka_service


@topics_router.post("/topics/")
def create_topic(request: CreateTopicRequest, service: KafkaService = Depends(get_kafka_service)):
    try:
        return service.create_topic(request.name, request.partitions, request.replication_factor)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@topics_router.post("/topics/{topic_name}/produce")
async def produce_to_topic(
    topic_name: str,
    num_messages: int = Query(10, description="Number of messages to produce"),
    interval: Optional[float] = Query(1.0, description="Interval between messages in seconds")
):
    """Bir topiğe rastgele mesajlar üretir."""
    if num_messages < 1:
        raise HTTPException(status_code=400, detail="Number of messages must be at least 1.")
    
    # Rastgele mesajları oluştur
    messages = generate_random_messages(num_messages)

    # Mesajları topiğe gönder
    try:
        response = kafka_service.produce_messages(topic_name, messages, interval)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Yeni bir endpoint ekleniyor
@topics_router.get("/topics/{topic_name}/consume")
async def consume_from_topic(
    topic_name: str,
    max_messages: int = Query(10, description="Maximum number of messages to consume"),
    timeout: Optional[float] = Query(1.0, description="Timeout for consuming messages in seconds"),
    background_tasks: BackgroundTasks = None,
):

    """Bir topikten mesaj tüketir."""
    if max_messages < 1:
        raise HTTPException(status_code=400, detail="Maximum number of messages must be at least 1.")
    
    # Mesajları tüket
    try:
        response = kafka_service.consume_messages(topic_name, timeout, max_messages)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@topics_router.post("/topics/{topic_name}/background_produce")
async def background_produce_to_topic(
    topic_name: str,
    num_messages: int = Query(10, description="Number of messages to produce"),
    interval: Optional[float] = Query(1.0, description="Interval between messages in seconds"),
    background_tasks: BackgroundTasks = None
):
    """Bir topiğe rastgele mesajlar üretir."""
    if num_messages < 1:
        raise HTTPException(status_code=400, detail="Number of messages must be at least 1.")
    
    # Arka planda mesaj üretimini başlat
    background_tasks.add_task(kafka_service.background_produce_messages, topic_name, num_messages, interval)
    
    return {"message": f"Messages are being produced to topic '{topic_name}' in the background."}


""" 
@topics_router.get("/topics/{topic_name}/consume_parallel")
async def consume_parallel_from_topic(
    topic_name: str,
    timeout: Optional[float] = Query(1.0, description="Timeout for consuming messages in seconds"),
):
    
    try:
        # Kafka service'i çağırıyoruz
        results = await kafka_service.run_consumers(topic_name, timeout)
        return {"messages": results}
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@topics_router.get("/consume-messages/{topic_name}", response_model=List[str])
async def consume_messages(topic_name: str):
    
    try:
        # Kafka'dan mesajları al
        await kafka_service.start_consumer(topic_name=topic_name)
        messages = [msg async for msg in kafka_service.consume_messages_()]
        return messages
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error consuming messages: {str(e)}") """





















