# Realtime-Data-Streaming

This project serves as a comprehensive guide to building an end-to-end data engineering pipeline. It covers each stage, from data ingestion to processing and storage, using a robust tech stack that ensures scalability and efficiency.

## Project Components

![architecture](https://github.com/user-attachments/assets/c99bc435-021c-4477-bc9a-e5efae83de35)

### Data Source

- Randomuser.me API: Provides random user data to simulate real-world data ingestion for the pipeline.

### Pipeline Orchestration

- Apache Airflow: Manages the workflow, orchestrating data ingestion and storing the fetched data in a PostgreSQL database.

### Real-Time Data Streaming

- Apache Kafka: Streams data from PostgreSQL to the data processing engine.

- Apache Zookeeper: Ensures distributed synchronization and coordination for Kafka.


## Data Processing

- Apache Spark: Processes the streamed data, leveraging its distributed computing capabilities.

## Data Storage

- Cassandra: Stores the processed data for efficient querying and analysis.

- PostgreSQL: Used for intermediate storage of ingested data before streaming.

## Key Features

1. Data Ingestion: Automates fetching data from an external API using Apache Airflow.

2. Streaming: Facilitates real-time data transfer with Apache Kafka.

3. Processing: Implements distributed data processing with Apache Spark.

4. Storage: Combines PostgreSQL and Cassandra for comprehensive storage solutions.


## What You'll Learn

- Setting up a data pipeline with Apache Airflow.
- Real-time data streaming with Apache Kafka.
- Distributed synchronization with Apache Zookeeper.
- Data processing techniques with Apache Spark.
- Data storage solutions with Cassandra and PostgreSQL.

## Tech Stack

- Apache Airflow
- Python
- Apache Kafka
- Apache Zookeeper
- Apache Spark
- Cassandra
- PostgreSQL

