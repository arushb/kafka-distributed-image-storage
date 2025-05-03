# Kafka Distributed Image Storage

This project implements a distributed image storage and retrieval system using Apache Kafka and Python. Images are uploaded, distributed, stored, and retrieved across several nodes, ensuring redundancy and reliability.

---

## Overview

The system consists of the following components:

- **Main Node:** Uploads images into the system.
- **Head Node:** Receives images, manages distribution to storage and backup (buddy) nodes, and handles metadata.
- **Buddy Node:** Stores backup copies of images and metadata for redundancy.
- **Storage Nodes:** Store the majority of images and acknowledge successful storage.
- **Retrieval Scripts:** Allow images to be retrieved from the system, regardless of where they are stored.

The nodes communicate using Kafka topics, enabling asynchronous and decoupled operation.

---

## File Structure

| File Name                   | Purpose                                           |
|-----------------------------|---------------------------------------------------|
| `main_node.py`              | Uploads images to the Kafka pipeline.             |
| `head_node.py`              | Distributes images and metadata, manages storage. |
| `buddy_node.py`             | Stores backup images and metadata.                |
| `storage_node_1.py`         | Stores assigned images, sends acknowledgments.    |
| `storage_node_2.py`         | Stores assigned images, sends acknowledgments.    |
| `head_node_retrieval.py`    | Coordinates image retrieval from nodes.           |
| `main_node_retrieval.py`    | Receives retrieved images at the Main Node.       |
| `storage_Node_retreival.py` | Handles retrieval requests at Storage Node 1.     |

---

## How It Works

1. **Image Upload:**  
   Place images in the directory specified in `main_node.py`. The Main Node encodes and sends them to the Kafka `image_topic`.

2. **Distribution:**  
   The Head Node receives images, stores them, and distributes them to Storage Nodes and the Buddy Node. Metadata is created for each image.

3. **Storage:**  
   Storage Nodes receive and store images, then send acknowledgments back via Kafka.

4. **Redundancy:**  
   The Buddy Node stores backup copies of both images and metadata.

5. **Retrieval:**  
   Use the retrieval scripts to fetch images from the system. The Head Node determines the location of each image and coordinates retrieval.

---

## Kafka Topics Used

- `image_topic`: Main Node → Head Node (image upload)
- `storage_topic`: Head Node → Storage Nodes (distribution)
- `buddy_update_topic`: Head Node → Buddy Node (backup)
- `ack_storage1_topic`, `ack_storage2_topic`: Storage Nodes → Head Node (acknowledgment)
- `main-retrieval`: Head/Storage Node → Main Node (retrieval)
- `storage-request-topic`: Head Node → Storage Node (retrieval request)

---

## Setup

1. **Requirements:**
   - Python 3.x
   - [confluent-kafka](https://github.com/confluentinc/confluent-kafka-python)
   - Apache Kafka running locally at `localhost:9092` (or update scripts as needed)

2. **Install dependencies:**
