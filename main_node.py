from confluent_kafka import Producer
import os
import base64

producer = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
        os.remove(msg.key())

def send_image(file_path):
    with open(file_path, 'rb') as f:
        image_data = base64.b64encode(f.read()).decode('utf-8')
    producer.produce('image_topic', key=file_path, value=image_data, callback=delivery_report)
    producer.poll(0)

image_directory = r"C:\Users\zahid\Downloads\pokemon_jpg"
for filename in os.listdir(image_directory):
    if filename.endswith(('.png', '.jpg', '.jpeg')):
        file_path = os.path.join(image_directory, filename)
        send_image(file_path)

producer.flush()
