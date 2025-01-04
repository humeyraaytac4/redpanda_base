from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from app.services.json_datas.json_data_generator import produce_messages_to_kafka, generate_stream_random_json

# Router oluştur
json_data_router = APIRouter()

# Mesaj üretim endpointi
@json_data_router.post("/generate-json/")
async def produce_messages(
    topic: str = Query(..., description="Kafka topic name"),
    num_messages: int = Query(10, description="Number of messages to produce"),
    interval: Optional[float] = Query(1, description="Interval between messages in seconds")
):
    if num_messages < 1:
        raise HTTPException(status_code=400, detail="Number of messages must be at least 1.")
    
    try:
        messages_sent = produce_messages_to_kafka(topic, num_messages, interval)
        return {"message": f"{messages_sent} messages successfully sent to topic '{topic}'."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to produce messages: {str(e)}")



@json_data_router.get("/generate_stream_random_json")
async def stream_json():
    response = await generate_stream_random_json()
    return response

