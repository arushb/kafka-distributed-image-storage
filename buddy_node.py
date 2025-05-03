from confluent_kafka import Consumer, Producer
import base64
import json
import os

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'buddy_node_group',
    'auto.offset.reset': 'earliest'
})
producer = Producer({'bootstrap.servers': 'localhost:9092'})

buddy_update_topic = 'buddy_update_topic'
buddy_data_directory = r"D:\kafka_2.13-3.5.0\Buddy\Rec_Images"
buddy_metadata_directory = r"D:\kafka_2.13-3.5.0\Buddy\Rec_Meta"
os.makedirs(buddy_data_directory, exist_ok=True)
os.makedirs(buddy_metadata_directory, exist_ok=True)

def process_and_store_buddy_data(key, value):
    key_str = key.decode('utf-8') if key else None
    
    if key_str.endswith('.json'):
        metadata = json.loads(value)
        metadata_path = os.path.join(buddy_metadata_directory, os.path.basename(key_str))
        with open(metadata_path, 'w') as meta_file:
            json.dump(metadata, meta_file)
    else:
        image_data = base64.b64decode(value)
        image_path = os.path.join(buddy_data_directory, os.path.basename(key_str))
        with open(image_path, 'wb') as img_file:
            img_file.write(image_data)

consumer.subscribe([buddy_update_topic])
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        break
    if msg.error():
        print(f"Error: {msg.error()}")
        break

    if msg.topic() == buddy_update_topic:
        process_and_store_buddy_data(msg.key(), msg.value())

consumer.close()