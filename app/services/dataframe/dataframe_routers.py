from fastapi import APIRouter, HTTPException
import os
import pandas as pd
from kafka import KafkaAdminClient, KafkaConsumer 
from kafka.errors import KafkaError
from app.services.dataframe.dataframe import consumer_group_lags_to_df, consumers_list, consumer_groups_lags_list

dataframe_router = APIRouter()

broker = os.getenv("KAFKA_BROKER", "redpanda:29092")
admin_client = KafkaAdminClient(bootstrap_servers=[broker])
consumer =  KafkaConsumer(bootstrap_servers=[broker],
    group_id="benthos-consumer-group")  


@dataframe_router.get("/consumers_list")
def list_consumers():
    try:
        # Listeye Kafka konularını ve tüketici gruplarını ekleme
        consumers= admin_client.list_consumer_groups()
        print(f"consumers:{consumers}",flush=True)
        
        # Her bir consumer grubunun adını alın
        consumer_names = [consumer[0] for consumer in consumers]
        print(f"consumer_names:{consumer_names}",flush=True)
        return consumer_names
    except KafkaError as e:
        raise Exception(f"Error fetching topics and consumers: {str(e)}")


""" @dataframe_router.get("/topics/consumers")
def get_consumer():
    try:
        # Belirli bir topic için consumer gruplarını döndür
        topics = consumer.assignment()
        print(f"consumer_topics:{topics}",flush=True)
        return topics
    except KafkaError as e:
        raise Exception(f"Error fetching consumers for topic '{topic}': {str(e)}")
 """


@dataframe_router.get("/topics/consumers")
def get_topic_from_consumer_groups():
    try:
        # Tüm tüketici gruplarını al
        group_list = consumers_list()
        group_details = admin_client.describe_consumer_groups(group_list)
        topic_list = []
        topics_list = []
        print(f"group_details:{group_details}",flush=True)
        for group_detail in group_details:
            group_name = group_detail.group
            print(f"group_name:{group_name}",flush=True)
            for member in group_detail.members:
                print(f"member:{member}",flush=True)
                member_id = member.member_id
                print(f"member_id:{member_id}",flush=True)
                # Her bir consumer üyesinin assignment (atanan konular) bilgilerini kontrol et
                if member.member_assignment:
                    assignment = member.member_assignment
                    print(f"member_assignment:{assignment}",flush=True)
                    print(f"assignment:{assignment}",flush=True)
                    topic_list = assignment.assignment
                    print(f"topic_list:{topic_list}",flush=True)

                    for topic_partition in assignment.assignment:
                        # Liste içindeki her bir öğe (topic, partitions) formatında olmalı
                        topic_name, partitions = topic_partition
                        topic_data = {
                            "group_name": group_name,
                            "member_id": member_id,
                            "topic_name": topic_name,
                            "partitions": partitions
                        }
                        topics_list.append(topic_data)
                        print(f"Processed topic: {topic_data}", flush=True)
        # Benzersiz topic'leri döndür
        return topics_list
    except KafkaError as e:
        raise HTTPException(status_code=500, detail=f"Error fetching topics for consumer '{group_name}': {str(e)}")


@dataframe_router.get("/consumer_group_offset")
def get_consumer_group_offset():
    try:
        # Kafka admin client ile consumer group için offset bilgilerini al
        group_offsets = admin_client.list_consumer_group_offsets("benthos-consumer-group")
        print(f"group_offsets:{group_offsets}", flush=True)

        # Veriyi doğru JSON formatına dönüştür
        lag_data = {}
        lag_data_list = []  # Tüm lag bilgilerini saklayacak liste
        for topic_partition, offset_metadata in group_offsets.items():
            topic = topic_partition.topic
            partition = topic_partition.partition
            offset = offset_metadata.offset
            lag_data = {
                            "group_name": "benthos-consumer-group",
                            "topic": topic,
                            "partition": partition,
                            "group_offsets": offset
                        }
            print(f"lag_data:{lag_data}", flush=True)
            lag_data_list.append(lag_data)  # Listeye ekle

        return {"lag_data": lag_data_list}  # Listeyi döndür

    except KafkaError as e:
        print(f"Error fetching offsets for consumer group {'benthos-consumer-group'}: {str(e)}")


@dataframe_router.get("/consumer_group_lags")
def get_consumer_group_lags():
    try:
        # Kafka admin client ile consumer group için offset bilgilerini al
        group_offsets = admin_client.list_consumer_group_offsets("benthos-consumer-group")
        print(f"group_offsets:{group_offsets}", flush=True)


        # End offsets almak için KafkaConsumer kullan
        lag_data = {}
        lag_data_list = []
        # Topic ve partition'ları al
        for topic_partition, offset_metadata in group_offsets.items():
            print(f"topic_partition:{topic_partition}", flush=True)
            topic = topic_partition.topic
            partition = topic_partition.partition
            group_offset = offset_metadata.offset

            # Kafka Consumer ile end_offset'i al
            consumer_ = KafkaConsumer(bootstrap_servers=[broker],
                group_id="benthos-consumer-group"
            )

            # End offset'i al
            end_offset_pre = consumer_.end_offsets([topic_partition])
            print(f"end_offset:{end_offset_pre}", flush=True)
            end_offset = consumer_.end_offsets([topic_partition])[topic_partition]
            print(f"end_offset:{end_offset}", flush=True)
            # Lag hesapla (end_offset - group_offset)
            lag = end_offset - group_offset

            # Sonuçları JSON formatında tut
            lag_data = {
                "group_name": "benthos-consumer-group",
                "topic": topic,
                "partition": partition,
                "group_offset": group_offset,
                "end_offset": end_offset,
                "lag": lag
            }
            print(f"lag_data:{lag_data}", flush=True)
            lag_data_list.append(lag_data)  # Listeye ekle

        return {"lag_data": lag_data_list}  # Listeyi döndür
    except KafkaError as e:
        print(f"Error fetching offsets for consumer group {'benthos-consumer-group'}: {str(e)}")


@dataframe_router.get("/consumer_groups_lags")
def get_consumer_groups_lags():
    try:
        group_list=consumers_list()

        # Kafka admin client ile consumer group için offset bilgilerini al
        group_offsets = admin_client.list_consumer_group_offsets("benthos-consumer-group")
        print(f"group_offsets:{group_offsets}", flush=True)

        # End offsets almak için KafkaConsumer kullan
        lag_data = {}
        lag_data_list = []
        # Topic ve partition'ları al
        for group_name in group_list:
            # Her bir grup için offset bilgilerini al
            try:
                group_offsets = admin_client.list_consumer_group_offsets(group_name)
                print(f"Offsets for group '{group_name}': {group_offsets}", flush=True)

                # Her grup için lag hesaplama
                for topic_partition, offset_metadata in group_offsets.items():
                    topic = topic_partition.topic
                    partition = topic_partition.partition
                    group_offset = offset_metadata.offset

                    # Kafka Consumer ile end_offset'i al
                    consumer_ = KafkaConsumer(
                        bootstrap_servers=[broker],
                        group_id=group_name
                    )

                    end_offset = consumer_.end_offsets([topic_partition])[topic_partition]
                    lag = end_offset - group_offset

                    # Verileri JSON formatında sakla
                    lag_data = {
                        "group_name": group_name,
                        "topic": topic,
                        "partition": partition,
                        "group_offset": group_offset,
                        "end_offset": end_offset,
                        "lag": lag
                    }
                    print(f"lag_data: {lag_data}", flush=True)
                    lag_data_list.append(lag_data)  # Listeye ekle

            except KafkaError as e:
                print(f"Error fetching offsets for group '{group_name}': {str(e)}", flush=True)

        # Tüm lag verilerini döndür
        return {"lag_data": lag_data_list}

    except KafkaError as e:
        print(f"Error fetching consumer groups: {str(e)}", flush=True)
        return {"error": f"Error fetching consumer groups: {str(e)}"}


@dataframe_router.get("/consumer_groups_lags_df")
def get_consumer_group_lags_df():
    try:
        data= consumer_groups_lags_list()
        print(f"data: {data}", flush=True)
        # DataFrame oluşturma
        df = pd.DataFrame(data['lag_data'])
        print(f"df: {df}", flush=True)
        return df
    except KafkaError as e:
        print(f"Error fetching consumer groups: {str(e)}", flush=True)
        return {"error": f"Error fetching consumer groups: {str(e)}"}


@dataframe_router.get("/last_df_to_dict")
def df_to_dict():
    try:
        df=consumer_group_lags_to_df()
        return df
    except KafkaError as e:
        return {"error": f"Error fetching consumer groups: {str(e)}"}