# Streaming-Data--Real-Time--Pipeline-Data

## Project Overview

This project creates a Kafka producer that fetches employee data from a public API
and sends it to Kafka topics for real-time processing using Spark and HDFS.
---
# Technologies Used
- Apache Kafka
- Docker Compose
- Apache Spark (PySpark)
- VS Code
- Hadoop Hdfs (Loacal host)
---
# Project Workflow

## Step 1: Start the Kafka cluster

Start the Kafka cluster before running any producers or consumers.
```
docker-compose up -d

```
## Step 2: Create Kafka Topics

### First topic for valid/successful messages
```
docker exec -it kafka1 kafka-topics --create \
--topic datalake \
--bootstrap-server kafka1:9092 \
--partitions 3 \
--replication-factor 3
```
### Sconed topic one for Dead Letter Queue topic(DLQ)
```
docker exec -it kafka1 kafka-topics --create \
--topic datalakedlq \
--bootstrap-server kafka1:9092 \
--partitions 3 \
--replication-factor 3
```
### Step 3: Run the Kafka Producer
```
python kafka_producer.py

```
### Step 4: Run the Spark Consumer and Read Data from HDFS
```
python spark_consumer.py

```

