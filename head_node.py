from confluent_kafka import Consumer, Producer
import base64
import json
import os

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'head_node_group',
    'auto.offset.reset': 'earliest'
})
producer = Producer({'bootstrap.servers': 'localhost:9092'})

image_topic = 'image_topic'
storage_topic = 'storage_topic'
ack_storage1_topic = 'ack_storage1_topic'
ack_storage2_topic = 'ack_storage2_topic'
buddy_update_topic = 'buddy_update_topic'

data_directory = r"D:\kafka_2.13-3.5.0\Head\Rec_Images"
metadata_directory = r"D:\kafka_2.13-3.5.0\Head\Rec_Meta"
os.makedirs(data_directory, exist_ok=True)
os.makedirs(metadata_directory, exist_ok=True)

def process_and_store_image(key, value):
    key_str = key.decode('utf-8') if key else None
    image_data = base64.b64decode(value)

    image_path = os.path.join(data_directory, os.path.basename(key_str))
    metadata_path = os.path.join(metadata_directory, os.path.basename(key_str) + '.json')
    
    with open(image_path, 'wb') as img_file:
        img_file.write(image_data)


    # metadata = {
    #     'filename': os.path.basename(key_str),
    #     'size': len(value),
    #     'destinations': ['head_node', 'buddy_node']
    # }

    # with open(metadata_path, 'w') as meta_file:
    #     json.dump(metadata, meta_file)

# def determine_destination_nodes(filename):
#     hash_value = hash(filename)
#     if hash_value % 2 == 0:
#         return ['head_node', 'buddy_node']
#     else:
#         return ['storage_node_1', 'storage_node_2']

def distribute_data():
    image_files = os.listdir(data_directory)
    total_images = len(image_files)
    images_for_storage = int(0.75 * total_images)
    
    for i, filename in enumerate(image_files):
        if i < images_for_storage:
            image_path = os.path.join(data_directory, filename)
            with open(image_path, 'rb') as img_file:
                image_data = base64.b64encode(img_file.read()).decode('utf-8')
            producer.produce(storage_topic, key=filename, value=image_data)
            
            #Editing code here:
            metadata_path = os.path.join(metadata_directory,filename+'.json')
            metadata = {
                'filename': filename,
                'size': len(image_data),
                'destinations': ['Storage_Node_1', 'Storage_Node_2']
            }

            with open(metadata_path, 'w') as meta_file:
                json.dump(metadata, meta_file)
            os.remove(image_path)
        else:
            image_path = os.path.join(data_directory, filename)
            with open(image_path, 'rb') as img_file:
                image_data = base64.b64encode(img_file.read()).decode('utf-8')
            producer.produce(buddy_update_topic, key=filename, value=image_data)
            metadata_path = os.path.join(metadata_directory,filename+'.json')
            metadata = {
                'filename': filename,
                'size': len(image_data),
                'destinations': ['Head_Node', 'Buddy_Node']
            }

            with open(metadata_path, 'w') as meta_file:
                json.dump(metadata, meta_file)
 
    producer.flush()

def send_metadata_to_buddy():
    metadata_files = os.listdir(metadata_directory)
    
    for filename in metadata_files:
        metadata_path = os.path.join(metadata_directory, filename)
        with open(metadata_path, 'r') as meta_file:
            metadata = json.load(meta_file)
            producer.produce(buddy_update_topic, key=filename.encode('utf-8'), value=json.dumps(metadata))
    
    producer.flush()

consumer.subscribe([image_topic])
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        break
    if msg.error():
        print(f"Error: {msg.error()}")
        break

    if msg.topic() == image_topic:
        process_and_store_image(msg.key(), msg.value())

distribute_data()
send_metadata_to_buddy()

consumer.close()
producer.flush()
