from confluent_kafka import Consumer, Producer
import os
import base64

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'storage_node_2',
    'auto.offset.reset': 'earliest'
})
producer = Producer({'bootstrap.servers': 'localhost:9092'})

storage_directory = r"D:\kafka_2.13-3.5.0\Storage_2\Rec_Images"
os.makedirs(storage_directory, exist_ok=True)

ack_storage2_topic = 'ack_storage2_topic'

def store_image_and_ack(key, value):
    key_str = key.decode('utf-8') if key else None
    image_data = base64.b64decode(value)
    image_path = os.path.join(storage_directory, os.path.basename(key_str))
    with open(image_path, 'wb') as img_file:
        img_file.write(image_data)
    producer.produce(ack_storage2_topic, key=key, value='Acknowledged')
    producer.poll(0)

consumer.subscribe(['storage_topic'])
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        break
    if msg.error():
        print(f"Error: {msg.error()}")
        break

    store_image_and_ack(msg.key(), msg.value())

consumer.close()
producer.flush()
