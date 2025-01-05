import requests
from confluent_kafka import Consumer, KafkaException, KafkaError


# ClickHouse HTTP API URL
CLICKHOUSE_URL = "http://clickhouse-server:8123/?database=logs_db"  # ClickHouse HTTP API'sinin adresi

# ClickHouse'da tabloyu oluştur
def create_table(topic_name: str):
    query = f"""
    CREATE TABLE IF NOT EXISTS {topic_name} (
        id UInt64,
        topic String,
        key Nullable(String),
        value String,
        partition Int32,
        offset Int64,
        timestamp DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY id;
    """
    try:
        response = requests.post(CLICKHOUSE_URL, data=query)
        response.raise_for_status()  # İstek başarısız olursa hata verir
        print(f"Table '{topic_name}' is ready.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to create table in ClickHouse: {e}")

# Kafka ayarları
KAFKA_CONFIG = {
    "bootstrap.servers": "redpanda:29092",
    "group.id": "clickhouse-consumer-group",
    "auto.offset.reset": "earliest",
}

# Kafka Consumer
consumer = Consumer(KAFKA_CONFIG)

def save_to_clickhouse(topic: str, message: dict):
    """
    Gelen mesajları ClickHouse'a kaydeder.
    """
    try:
        query = f"""
        INSERT INTO {topic} (id, topic, key, value, partition, offset, timestamp)
        VALUES ({message["id"]}, '{topic}', '{message["key"]}', '{message["value"]}', {message["partition"]}, {message["offset"]}, now())
        """
        # ClickHouse'a HTTP POST isteği gönder
        response = requests.post(CLICKHOUSE_URL, data=query)
        response.raise_for_status()  # İstek başarısız olursa hata verir
        print(f"Message logged successfully: {message}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while saving to ClickHouse: {e}")

def consume_messages(topic: str, timeout: float = 1.0, max_messages: int = 10):
    """
    Belirli bir topikten mesaj tüketir ve ClickHouse'a kaydeder.
    """
    consumer.subscribe([topic])
    messages = []

    try:
        for i in range(max_messages):
            msg = consumer.poll(timeout)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            
            message = {
                "id": i + 1,  # Her mesaj için bir ID belirle
                "key": msg.key().decode("utf-8") if msg.key() else None,
                "value": msg.value().decode("utf-8"),
                "partition": msg.partition(),
                "offset": msg.offset()
            }
            messages.append(message)
            
            # ClickHouse'a kaydet
            try:
                save_to_clickhouse(topic, message)
            except Exception as e:
                print(f"Failed to log message to ClickHouse: {e}")
        
        return {"messages": messages}
    except Exception as e:
        raise Exception(f"Failed to consume messages from topic '{topic}': {e}")
