import os
from kafka import KafkaConsumer, TopicPartition

# =========================
# Função Conectar no Kafka
# =========================
def connect_kafka():

    kafka_instance = os.getenv('KAFKA_INSTANCE')
    topic = os.getenv('KAFKA_TOPIC')

    consumer = KafkaConsumer(
        bootstrap_servers=kafka_instance,
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
        api_version_auto_timeout_ms=10000,
        request_timeout_ms=30000,
        metadata_max_age_ms=10000
    )

    return consumer, topic


# =========================
# Função Obter último offset de cada partição
# =========================
def get_last_offsets(consumer, topic, partitions):

    topic_partitions = [TopicPartition(topic, p) for p in partitions]
    end_offsets = consumer.end_offsets(topic_partitions)

    for tp, end_offset in end_offsets.items():
        last_offset = end_offset - 1
        print(f"Partition {tp.partition} | Last offset: {last_offset}")


# =========================
# Função principal (Handler)
# =========================
def handler(event, context):

    consumer = None

    try:

        # Conecta no Kafka
        consumer, topic = connect_kafka()

        # Obtém partições do Tópico
        partitions = consumer.partitions_for_topic(topic)

        if not partitions:
            raise Exception("Nenhuma partição encontrada")

        # Obter último offset de cada partição 
        get_last_offsets(consumer, topic, partitions)

        return {
            "statusCode": 200,
            "partitions": list(partitions)
        }

    except Exception as e:

        print("Erro:", str(e))

        return {
            "statusCode": 500,
            "error": str(e)
        }

    finally:
        if consumer:
            try:
                consumer.close()
            except:
                pass