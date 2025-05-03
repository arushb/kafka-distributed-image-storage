# import confluent_kafka as ck
# import os
# import time

# # Kafka broker configuration
# bootstrap_servers = 'localhost:9092'
# group_id = 'storage_node_1_retrieval_group'
# auto_offset_reset = 'earliest'

# # Topics
# storage_request_topic = 'storage-request-topic'
# main_retrieval_topic = 'main-retreival'

# # Directories
# data_directory = r"D:\kafka_2.13-3.5.0\Storage_1\Rec_Images"

# # Create Kafka consumer and producer
# consumer = ck.Consumer({
#     'bootstrap.servers': bootstrap_servers,
#     'group.id': group_id,
#     'auto.offset.reset': auto_offset_reset
# })

# producer = ck.Producer({
#     'bootstrap.servers': bootstrap_servers
# })

# # Retrieve data and send to Main Node
# while True:
#     msg = consumer.poll(1.0)

#     if msg is None:
#         continue

#     if msg.error():
#         print(f"Error: {msg.error()}")
#         continue

#     filename = msg.key().decode('utf-8')
#     data_path = os.path.join(data_directory, filename)

#     with open(data_path, 'rb') as data_file:
#         data = data_file.read()

#     producer.produce(main_retrieval_topic, key=filename.encode('utf-8'), value=data)

#     # Remove data from local storage
#     os.remove(data_path)

import confluent_kafka as ck
import os
import time

# Kafka broker configuration
bootstrap_servers = 'localhost:9092'
group_id = 'storage_node_1_retrieval_group'
auto_offset_reset = 'earliest'

# Topics
storage_request_topic = 'storage-request-topic'
main_retrieval_topic = 'main-retrieval'  # Corrected topic name

# Directories
data_directory = r"D:\kafka_2.13-3.5.0\Storage_1\Rec_Images"

# Create Kafka consumer and producer
consumer = ck.Consumer({
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
    'auto.offset.reset': auto_offset_reset
})

producer = ck.Producer({
    'bootstrap.servers': bootstrap_servers,
    'acks': 'all'  # Wait for all replicas to acknowledge
})

# Retrieve data and send to Main Node
while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        print(f"Error: {msg.error()}")
        continue

    filename = msg.key().decode('utf-8')
    data_path = os.path.join(data_directory, filename)

    with open(data_path, 'rb') as data_file:
        data = data_file.read()

    producer.produce(main_retrieval_topic, key=filename.encode('utf-8'), value=data)

    # Remove data from local storage
    os.remove(data_path)
    producer.flush()  # Ensure that all messages are sent before continuing

