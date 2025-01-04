from elasticsearch import Elasticsearch, exceptions as es_exceptions
from confluent_kafka import Consumer, KafkaException, KafkaError


# Elasticsearch bağlantısı
es = Elasticsearch(hosts=["http://elasticsearch:9200"])

# Kafka ayarları
KAFKA_CONFIG = {
    "bootstrap.servers": "redpanda:29092",
    "group.id": "elastic-consumer-group",
    "auto.offset.reset": "earliest",
}

# Kafka Consumer
consumer = Consumer(KAFKA_CONFIG)


def save_to_elasticsearch(topic: str, message: dict):
    """
    Gelen mesajları Elasticsearch'e kaydeder.
    """
    index_name = f"topic-logs-{topic.lower()}"
    doc = {
        "topic": topic,
        "message": message,
    }
    try:

        response = es.index(index=index_name, document=doc)
        return {"result": response["result"], "id": response["_id"]}
    except es_exceptions.ConnectionError:
        raise Exception("Failed to connect to Elasticsearch")
    except Exception as e:
        raise Exception(f"An error occurred while saving to Elasticsearch: {e}")


def consume_messages(topic: str, timeout: float = 1.0, max_messages: int = 10):
    """
    Belirli bir topikten mesaj tüketir ve Elasticsearch'e kaydeder.
    """
    consumer.subscribe([topic])
    messages = []

    try:
        for _ in range(max_messages):
            msg = consumer.poll(timeout)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            
            message = {
                "key": msg.key().decode("utf-8") if msg.key() else None,
                "value": msg.value().decode("utf-8"),
                "partition": msg.partition(),
                "offset": msg.offset()
            }
            messages.append(message)
            
            # Elasticsearch'e kaydet
            try:
                es_log = save_to_elasticsearch(topic, message)
                print(f"Message logged successfully: {es_log}", flush= True)
            except Exception as e:
                print(f"Failed to log message to Elasticsearch: {e}")
        
        return {"messages": messages}
    except Exception as e:
        raise Exception(f"Failed to consume messages from topic '{topic}': {e}")
