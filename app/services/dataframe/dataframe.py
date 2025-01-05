import os
from kafka import KafkaAdminClient, KafkaConsumer 
from kafka.errors import KafkaError
from datetime import datetime
import pandas as pd


broker = os.getenv("KAFKA_BROKER", "redpanda:29092")
admin_client = KafkaAdminClient(bootstrap_servers=[broker])

def get_consumer_group_list():
    """Tüm tüketici gruplarını listele."""
    try:
        consumers = admin_client.list_consumer_groups()
        return [consumer[0] for consumer in consumers]
    except KafkaError as e:
        raise Exception(f"Error fetching consumer groups: {str(e)}")


def get_group_offsets(group_name):
    """Belirli bir tüketici grubunun offset bilgilerini al."""
    try:
        return admin_client.list_consumer_group_offsets(group_name)
    except KafkaError as e:
        raise Exception(f"Error fetching offsets for group '{group_name}': {str(e)}")


def calculate_lag(group_name, group_offsets):
    """Bir tüketici grubunun lag değerlerini hesapla."""
    lag_data_list = []
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=[broker],
            group_id=group_name
        )

        for topic_partition, offset_metadata in group_offsets.items():
            topic = topic_partition.topic
            partition = topic_partition.partition
            group_offset = offset_metadata.offset

            # End offset al
            end_offset = consumer.end_offsets([topic_partition])[topic_partition]
            lag = end_offset - group_offset

            # Lag verilerini ekle
            lag_data_list.append({
                "group_name": group_name,
                "topic": topic,
                "partition": partition,
                "lag": lag
            })
    except KafkaError as e:
        print(f"Error calculating lag for group '{group_name}': {str(e)}", flush=True)
    
    return lag_data_list


def get_consumer_groups_lags():
    """Tüm tüketici gruplarının lag değerlerini al."""
    try:
        consumer_groups = get_consumer_group_list()
        all_lag_data = []

        for group_name in consumer_groups:
            try:
                group_offsets = get_group_offsets(group_name)
                lag_data = calculate_lag(group_name, group_offsets)
                all_lag_data.extend(lag_data)
            except Exception as e:
                print(f"Error processing group '{group_name}': {str(e)}", flush=True)

        return {"lag_data": all_lag_data}

    except Exception as e:
        print(f"Error fetching consumer groups lags: {str(e)}", flush=True)
        return {"error": f"Error fetching consumer groups lags: {str(e)}"}


def consumer_group_lags_to_df():
    """Tüm tüketici gruplarının lag değerlerini bir DataFrame olarak döndür."""
    try:
        data = get_consumer_groups_lags()

        # DataFrame oluşturma
        df = pd.DataFrame(data['lag_data'])
        
        # Gruplama ve işlem
        result = (
            df.groupby(['group_name', 'topic'])
            .agg({
                'partition': list,
                'lag': 'sum'
            })
            .reset_index()
        )

        # Zaman damgası ekleme
        current_timestamp = datetime.utcnow().isoformat()
        result['timestamp'] = current_timestamp

        result = result[['timestamp', 'topic', 'group_name',  'partition', 'lag']]

        # Sonuçları sözlük formatına çevirme
        result_dict = {
            "result": result.to_dict(orient='records')
        }

        return result_dict

    except KafkaError as e:
        print(f"Error fetching consumer groups: {str(e)}", flush=True)
        return {"error": f"Error fetching consumer groups: {str(e)}"}


def consumers_list():
    try:
        # Listeye Kafka konularını ve tüketici gruplarını ekleme
        consumers= admin_client.list_consumer_groups()
        # Her bir consumer grubunun adını alın
        consumer_names = [consumer[0] for consumer in consumers]
        return consumer_names
    except KafkaError as e:
        raise Exception(f"Error fetching topics and consumers: {str(e)}")
    
def consumer_groups_lags_list():
    try:
        group_list=consumers_list()

        # End offsets almak için KafkaConsumer kullan
        lag_data = {}
        lag_data_list = []
        # Topic ve partition'ları al
        for group_name in group_list:
            # Her bir grup için offset bilgilerini al
            try:
                group_offsets = admin_client.list_consumer_group_offsets(group_name)

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
                    lag_data_list.append(lag_data)  # Listeye ekle

            except KafkaError as e:
                print(f"Error fetching offsets for group '{group_name}': {str(e)}", flush=True)

        # Tüm lag verilerini döndür
        return {"lag_data": lag_data_list}

    except KafkaError as e:
        print(f"Error fetching consumer groups: {str(e)}", flush=True)
        return {"error": f"Error fetching consumer groups: {str(e)}"}








