# 🏙️ Smart City Real-Time Data Pipeline

![Python](https://img.shields.io/badge/Python-3.9-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.0-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.6.0-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20Glue%20%7C%20Athena-232F3E?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED?style=for-the-badge&logo=docker&logoColor=white)

An End-to-End Data Engineering project simulating a real-time Smart City IoT infrastructure. This pipeline generates synthetic vehicle/traffic data, ingests it via **Kafka**, processes it using **Apache Spark Structured Streaming**, and stores it in **AWS S3** for analysis using **AWS Glue** and **Athena**.

---

## 🏗️ Architecture

![Architecture Diagram](screenshots/architecture.png)
*(Please upload an architecture diagram here. See "What I need from you" below)*

**Data Flow:**
1.  **IoT Devices (Python):** Simulates 5 streams (Vehicle info, GPS, Traffic Cameras, Weather, Emergency).
2.  **Message Broker (Kafka):** Buffers data using Zookeeper & Broker managers.
3.  **Stream Processing (Spark):** Reads from Kafka, processes schema (Avro), and handles watermarking.
4.  **Data Lake (AWS S3):** Stores raw data in Parquet format.
5.  **Catalog (AWS Glue):** Crawls S3 to define the schema.
6.  **Analysis (AWS Athena):** SQL-based querying on the Data Lake.
7.  **Visualization (AWS QuickSight):** Geospatial dashboards.

---

## 🛠️ Technologies Used

-   **Language:** Python (PySpark, Confluent-Kafka)
-   **Ingestion:** Apache Kafka, Zookeeper
-   **Processing:** Apache Spark Structured Streaming
-   **Format:** Avro (Serialization), Parquet (Storage)
-   **Cloud (AWS):** S3, Glue, Athena, IAM
-   **Containerization:** Docker, Docker Compose

---

## 📂 Project Structure

```bash
IOT-SmartCity-RealTime-Pipeline/
├── docker/
│   └── docker-compose.yaml     # Kafka & Spark Cluster Setup
│   └── Dockerfile
├── jobs/
│   └── spark-city.py           # Spark Streaming Logic (S3 Write)
│   └── producer.py             # IOT data producer
├── screenshots/                # Project Demos
├── .env.example                # Example credentials file
├── .gitignore
├── README.md
└── requirements.txt            # Dependencies
```
---

## 🚀 How to Run

### 1. Prerequisites

* **Docker Desktop** (Make sure it is running with at least 4GB RAM allocated).
* **AWS Account** with an S3 Bucket created.
* **Python 3.9+** installed locally.

### 2. Setup Environment

Clone the repository:

```bash
git clone https://github.com/Pawarkunal/IOT-SmartCity-RealTime-Pipeline.git
cd IOT-SmartCity-RealTime-Pipeline

```

Create a `.env` file in the root directory (copy from example):

```bash
# Create .env file and add your AWS keys
AWS_ACCESS_KEY=your_access_key_here
AWS_SECRET_KEY=your_secret_key_here

```

### 3. Start Infrastructure (Docker)

This spins up Kafka, Zookeeper, Spark Master, and Spark Worker.

```bash
docker-compose up -d --build

```

### 4. Run the Pipeline

**Step A: Submit Spark Job**
Submit the Spark job to the cluster. This command includes the necessary AWS and Kafka dependencies.

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.12.150,org.apache.spark:spark-avro_2.12:3.5.1 \
  /opt/spark-jobs/smart-city.py

```

**Step B: Start Data Generation**
Run the Python script to simulate IoT devices sending data to Kafka.

```bash
# Install local dependencies first
pip install -r requirements.txt

# Run the producer
python /jobs/producer.py

```

---

## 📊 Results

### 1. AWS S3 (Data Lake)

Raw data landing in S3 bucket, organized by topic.

### 2. AWS Athena (SQL Analysis)

Querying the raw Parquet data directly using SQL.

---

## 👤 Author

**Kunal Pawar**

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/kunalpawar-in)
[![GitHub](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)](https://github.com/Pawarkunal)

---

## 🤝 Contributing & Feedback

Contributions, issues, and feature requests are welcome!
If you found this project helpful, please give it a ⭐️ on GitHub.

```
