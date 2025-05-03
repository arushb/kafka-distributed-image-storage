# import confluent_kafka as ck
# import os
# import json
# import time

# # Kafka broker configuration
# bootstrap_servers = 'localhost:9092'
# group_id = 'head_node_retrieval_group'
# auto_offset_reset = 'earliest'

# # Topics
# main_retrieval_topic = 'main-retreival'
# storage_request_topic = 'storage-request-topic'

# # Directories
# metadata_directory = r"D:\kafka_2.13-3.5.0\Head\Rec_Meta"
# retrieved_directory = r"D:\kafka_2.13-3.5.0\Head\Rec_Images"
# os.makedirs(retrieved_directory, exist_ok=True)

# # Create Kafka consumer and producer
# consumer = ck.Consumer({
#     'bootstrap.servers': bootstrap_servers,
#     'group.id': group_id,
#     'auto.offset.reset': auto_offset_reset
# })

# producer = ck.Producer({
#     'bootstrap.servers': bootstrap_servers
# })

# # Retrieve and send data every 2 minutes
# while True:
#     metadata_files = os.listdir(metadata_directory)
#     metadata_files.sort()

#     for i in range(10):
#         if i >= len(metadata_files):
#             break

#         metadata_path = os.path.join(metadata_directory, metadata_files[i])
#         with open(metadata_path, 'r') as meta_file:
#             metadata = json.load(meta_file)

#         if metadata['destinations'] == ['Head_Node']:
#             # Send data to Main Node
#             filename = metadata['filename']
#             data_path = os.path.join(retrieved_directory, filename)
#             # print(data_path)
#             with open(data_path, 'rb') as data_file:
#                 data = data_file.read()

#             producer.produce(main_retrieval_topic, key=filename.encode('utf-8'), value=data)

#             # Remove data and metadata from local storage
#             os.remove(data_path)
#             os.remove(metadata_path)
#         else:
#             # Send request to Storage Node 1
#             producer.produce(storage_request_topic, key=metadata['filename'].encode('utf-8'), value=metadata['destinations'][0])
#             metadata_path = os.path.join(metadata_directory, metadata_files[i])
#             os.remove(metadata_path)
#     time.sleep(20)

import confluent_kafka as ck
import os
import json
import time
import base64

# Kafka broker configuration
bootstrap_servers = 'localhost:9092'
group_id = 'head_node_retrieval_group'
auto_offset_reset = 'earliest'

# Topics
main_retrieval_topic = 'main-retrieval'
storage_request_topic = 'storage-request-topic'

# Directories
metadata_directory = r"D:\kafka_2.13-3.5.0\Head\Rec_Meta"
retrieved_directory = r"D:\kafka_2.13-3.5.0\Head\Rec_Images"
# os.makedirs(retrieved_directory, exist_ok=True)

# Create Kafka consumer and producer
consumer = ck.Consumer({
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
    'auto.offset.reset': auto_offset_reset
})

producer = ck.Producer({
    'bootstrap.servers': bootstrap_servers,
    # 'acks': 'all'  # Wait for all replicas to acknowledge
})

# Retrieve and send data every 2 minutes
while True:
    metadata_files = os.listdir(metadata_directory)
    metadata_files.sort()

    for i in range(10):
        if i >= len(metadata_files):
            break

        metadata_path = os.path.join(metadata_directory, metadata_files[i])
        with open(metadata_path, 'r') as meta_file:
            metadata = json.load(meta_file)
        print(metadata['destinations'])
        if metadata['destinations'] == ['Head_Node', 'Buddy_Node']:
            # Send data to Main Node
            filename = metadata['filename']
            data_path = os.path.join(retrieved_directory, filename)
            with open(data_path, 'rb') as data_file:
                data = data_file.read()
                

            producer.produce(main_retrieval_topic, key=filename.encode('utf-8'), value=data)

            # Remove data and metadata from local storage

            print(data_path)
            os.remove(data_path)
            print("Head")
            os.remove(metadata_path)
            producer.flush()  # Ensure that all messages are sent before continuing
        else:
            # Send request to Storage Node 1
            producer.produce(storage_request_topic, key=metadata['filename'].encode('utf-8'), value=metadata['destinations'][0])
            metadata_path = os.path.join(metadata_directory, metadata_files[i])
            os.remove(metadata_path)
            print("Storage")
            producer.flush()  # Ensure that all messages are sent before continuing
    time.sleep(20)

