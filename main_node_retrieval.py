import confluent_kafka as ck
import os

# Kafka broker configuration
bootstrap_servers = 'localhost:9092'
group_id = 'main_node_retrieval_group'
auto_offset_reset = 'earliest'

# Topics
main_retrieval_topic = 'main-retrieval'

# Directory for retrieved data
retrieved_directory = r"D:\kafka_2.13-3.5.0\Main\Retrieved"
os.makedirs(retrieved_directory, exist_ok=True)

# Create Kafka consumer
consumer = ck.Consumer({
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
    'auto.offset.reset': auto_offset_reset
})

# Consume and store retrieved data
while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        print(f"Error: {msg.error()}")
        continue

    filename = msg.key().decode('utf-8')
    data = msg.value()

    with open(os.path.join(retrieved_directory, filename), 'wb') as data_file:
        data_file.write(data)
