import json
import time
from faker import Faker
from confluent_kafka import Producer
from fastapi.responses import StreamingResponse
import random
import json
from datetime import datetime

# Rastgele veri üretimi için Faker
fake = Faker()

# Kafka Producer ayarları
producer_conf = {
    "bootstrap.servers": "redpanda:29092",  # Kafka broker bilgisi
    "client.id": "json-data-generator"
}
producer = Producer(producer_conf)

# Rastgele JSON veri üretme fonksiyonu
def generate_random_json():
    return {
        "id": fake.uuid4(),
        "name": fake.name(),
        "email": fake.email(),
        "created_at": fake.date_time_this_year().isoformat()
    }

# Mesaj gönderme callback'i
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Mesaj üretim fonksiyonu
def produce_messages_to_kafka(topic: str, num_messages: int, interval: float):
    messages_sent = 0
    for _ in range(num_messages):
        data = generate_random_json()
        producer.produce(
            topic,
            key=str(data["id"]),
            value=json.dumps(data),
            callback=delivery_report
        )
        producer.flush()
        time.sleep(interval)
        messages_sent += 1
    return messages_sent


def generate_random_messages(num_messages: int):
    return [
        {
            "id": fake.uuid4(),
            "name": fake.name(),
            "email": fake.email(),
            "created_at": fake.date_time_this_year().isoformat(),
            "address": fake.address(),
            "phone_number": fake.phone_number(),
            "company": fake.company(),
            "job_title": fake.job(),
            "ssn": fake.ssn(),
            "city": fake.city(),
            "state": fake.state(),
            "zip_code": fake.zipcode()
        }
        for _ in range(num_messages)
    ]


# Rastgele JSON verisi üreten async fonksiyon
async def generate_random_json_s():
    """Sürekli olarak rastgele JSON verileri üretir ve Kafka'ya gönderir."""
    while True:
        # Rastgele JSON verisi oluştur
        random_data = {
            "id": random.randint(1, 100),
            "value": random.uniform(0, 100),
            "status": random.choice(["active", "inactive", "pending"]),
            "timestamp": datetime.utcnow().isoformat()  # Anlık zaman UTC formatında
        }
        # JSON'u Kafka'ya gönder
        try:
             # JSON'u döndür ve biraz bekle
            yield f"event: Result\n"
            yield f"data: {json.dumps(random_data)}\n\n"
            print(f"{random_data}", flush=True)
        except Exception as e:
            yield f"event: Error\n"
            yield f"data: {json.dumps({'detail' : 'Error generating random json', 'error': {e}})}"
            print(f"Error generating random json: {e}", flush=True)

      

async def generate_stream_random_json():
    """Rastgele JSON verilerini sürekli olarak Kafka'ya gönderir."""
    return StreamingResponse(generate_random_json_s(),
            media_type="text/event-stream",
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-store",
            },
    )