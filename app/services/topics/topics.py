from confluent_kafka.admin import AdminClient, NewTopic
from fastapi import HTTPException
from confluent_kafka import Producer
import json
import time
from confluent_kafka import Consumer
from app.services.json_datas.json_data_generator import generate_random_messages
""" import asyncio
from confluent_kafka import TopicPartition
from aiokafka import AIOKafkaConsumer
from kafka import KafkaConsumer """


class KafkaService:
    def __init__(self, broker: str):
        self.broker = broker
        # Yeni bir AdminClient başlatıyoruz
        self.admin_client = AdminClient({"bootstrap.servers": broker, 
                                        "client.id": "create-topics"})        
        self.producer = Producer({"bootstrap.servers": broker, 
                                  "client.id": "produce-messages"
                                  })
        self.consumer = Consumer({
            "bootstrap.servers": broker,
            "group.id": "fastapi-consumer-group",
            "auto.offset.reset": "earliest"
        })
        self.consumer_parallel = Consumer({
            "bootstrap.servers": broker,
            "group.id": "parallel-consumer-group",
            "auto.offset.reset": "earliest"
        })


        
    def create_topic(self, name: str, partitions: int, replication_factor: int):
        topic = NewTopic(name, num_partitions=partitions, replication_factor=replication_factor)
        fs = self.admin_client.create_topics([topic])
        for topic_name, future in fs.items():
            try:
                future.result()  # Topic'in başarıyla oluşturulup oluşturulmadığını kontrol eder
                return {"message": f"Topic '{name}' created successfully."}
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Failed to create topic '{name}': {e}")


    def produce_messages(self, topic: str, messages: list[dict], interval: float = 1.0):
        """Bir topiğe mesaj üretir."""
        def delivery_report(err, msg):
            if err is not None:
                print(f"Message delivery failed: {err}")
            else:
                print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

        try:
            for message in messages:
                self.producer.produce(
                    topic,
                    key=str(message.get("id", "")),
                    value=json.dumps(message),
                    callback=delivery_report
                )
                self.producer.flush()  # Mesajın gönderimini tamamla
                time.sleep(interval)
            return {"message": f"{len(messages)} messages successfully sent to topic '{topic}'."}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to produce messages to topic '{topic}': {e}")
        

        
    def consume_messages(self, topic: str, timeout: float = 1.0, max_messages: int = 10):
        #Belirli bir topikten mesaj tüketir.
        self.consumer.subscribe([topic])
        messages = []

        try:
            for _ in range(max_messages):
                msg = self.consumer.poll(timeout)
                if msg is None:
                    break
                if msg.error():
                    raise Exception(f"Consumer error: {msg.error()}")
                messages.append({
                    "key": msg.key().decode("utf-8") if msg.key() else None,
                    "value": msg.value().decode("utf-8"),
                    "partition": msg.partition(),
                    "offset": msg.offset()
                })
            return {"messages": messages}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to consume messages from topic '{topic}': {e}")

    async def background_produce_messages(self,topic_name: str, num_messages: int, interval: float):              
        try:
            # Rastgele mesajları oluştur
            messages = generate_random_messages(num_messages)
            
            # Mesajları topiğe gönder
            self.produce_messages(topic_name, messages, interval)
        except Exception as e:
            # Hata durumunda loglama yapılabilir
            print(f"Error producing messages to topic {topic_name}: {e}")



"""
    def get_topic_partitions(self, topic: str):

        metadata = self.admin_client.list_topics(topic=topic, timeout=10)
        topic_metadata = metadata.topics.get(topic)
        print(f"topic_metadata:{topic},{topic_metadata.partitions}", flush=True)
        if topic_metadata is None:
            raise HTTPException(status_code=404, detail=f"Topic '{topic}' not found.")
        return len(topic_metadata.partitions)
    
    def create_consumers(self, topic: str):
        # Topic'in partition sayısını alıyoruz
        partition_count = self.get_topic_partitions(topic)
        
        consumers = []
        
        # Partition sayısı kadar consumer oluşturuyoruz
        for i in range(partition_count):
            consumer = KafkaConsumer(
            topic,
            bootstrap_servers = self.broker,
            group_id="my_consumer_group",
            partition_assignment_strategy='roundrobin',  # Or use 'range'
            auto_offset_reset='earliest'
        )
            partition = TopicPartition(topic, i)  # topic ve partition numarasını belirtiyoruz

            consumer.assign([partition])  # Her consumer'ı bir partitiona atıyoruz
            consumers.append(consumer)
        print(f"consumers:{consumers}", flush=True)
        return consumers

    async def consume_messages_async(self, consumer, timeout: float = 1.0):

        try:
            while True:
                msg = consumer.poll(timeout)
                if msg is None:
                    break
                
                if msg.error():
                    raise Exception(f"Consumer error: {msg.error()}")
                
                # Mesaj bilgisini döndür
                return {
                    "key": msg.key().decode("utf-8") if msg.key() else None,
                    "value": msg.value().decode("utf-8"),
                    "partition": msg.partition(),
                    "offset": msg.offset()
                }
        except Exception as e:
            return {"error": str(e)}


   
    async def run_consumers(self, topic: str, timeout: float = 1.0):

        consumers = self.create_consumers(topic)
        tasks = [
            asyncio.create_task(self.consume_messages_async(consumer, timeout))
            for consumer in consumers
        ]
        print(f"tasks:{tasks}",flush=True)
        # Tüm consumer'ların sonuçlarını bekliyoruz
        results = await asyncio.gather(*tasks)
        print(f"results:{results}", flush=True)
        return results

 """

"""     async def start_consumer(self,topic_name:str):
        self.consumer_aio = AIOKafkaConsumer(
            topic_name,
            bootstrap_servers=self.broker,
            group_id="aio_consumer"
        )
        await self.consumer_aio.start()

    async def consume_messages(self):
        if not self.consumer_aio:
            raise HTTPException(status_code=500, detail="Consumer not started yet.")
        try:
            async for msg in self.consumer_aio:
                # Mesajları burada işleyip döndürebiliriz
                print(f"Consumed message: {msg.topic} - {msg.partition} - {msg.offset} - {msg.key} - {msg.value.decode('utf-8')}", flush=True)
                yield msg.value.decode('utf-8')  # İstediğiniz gibi döndürebilirsiniz
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            await self.consumer_aio.stop()
 """


