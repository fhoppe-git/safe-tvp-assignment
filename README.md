# Safe - Senior Data Engineer Take-Home Exploration

## Overview
This project implements an ETL pipeline to analyze on-chain data from Ethereum Mainnet. 
The focus is on outgoing Total Value Processed (TVP) for Safe accounts, and the data is extracted, transformed, and stored in a Parquet file format. 
This pipeline includes a top-K analysis for the highest transaction volume and count across various Safe TVP verticals and protocols.

## Table of Contents
- [Requirements](#requirements)
- [Project Structure](#project-structure)
- [Setup](#setup)
- [Execution](#execution)
- [Assumptions](#assumptions)
- [Approach](#approach)
- [Results](#results)
- [Docker Deployment](#docker-deployment)
- [Bonus Task Automation](#bonus-task-automation)

## Requirements
- Python 3.8+
- Dune API Access with API Key
- Docker (for containerization)

## Project Structure

- **pipeline**: Includes main.py and other files for the docker container.
- **queries**: Includes the SQL code for the Dune queries.


## Setup
### 1. **Clone the repository:**
```bash
git clone https://github.com/yourusername/safe-tvp-assignment.git
```
   
### **2. Install the dependencies:**
```bash
cd safe-tvp-assignment/pipeline
pip install -r requirements.txt
```
   
### **3. Set up environment variables for Dune API access (if required):**
```bash
export DUNE_API_KEY="your_dune_api_key"
```

## Execution
**Run the ETL pipeline and analysis:**
```bash
cd safe-tvp-assignment/pipeline
python main.py
```

## Assumptions
- The timeframe was not specified in the assignment, so i included parameter to set start and enddate in the query.
- I used rank to determine the Top5, so if two rows have the same number both rows would be included, other options like dense rank would also be possible.


## Approach
### 1. **Extract**
   - Extract on-chain data using the Dune API for the specified query ID. The parameters include a date range for filtering the data on Ethereum Mainnet.

### 2. **Transform**
   - Convert raw data into summarized data at weekly intervals, aggregated by:
     - Vertical
     - Protocol
   - Calculations include:
     - Unique Safes
     - Total transactions per week
     - Outgoing TVP in USD per week

### 3. **Load**
   - Save the transformed data in Parquet format for efficient storage and retrieval.
   - Load the data into a new Dune table for additional querying.

### 4. **Top K Analysis**
   - Identify the Top 5 TVP verticals and protocols based on:
     - Highest transaction volume
     - Highest transaction count
     
## Results
Results are stored in separate Parquet files for each aggregation type:
- `vertical_summary.parquet`
- `protocol_summary.parquet`

The top 5 results for each category are displayed in the console.


## Docker Deployment
To deploy this project using Docker:
### 1. **Build the Docker image**:
```bash
docker build -t safe-tvp-assignment .
```
### 2. **Run the container**:
```bash
docker run --env-file .env safe-tvp-assignment
```
### 3. **Environment Configuration**:
   - Ensure that your `.env` file with the Dune API key is available in the project root and referenced in the Docker container using the `--env-file` option.

This containerization enables easy deployment and ensures consistent environments for running the ETL pipeline.


## Bonus Task Automation
For smaller task AWS Lambda or similar cloud functions are suitable, but to create more complex and scalable piplines i would prefer Airflow for several reasons:
   
### **Airflow advantages**
#### 1. **Complex Workflow Management**
- Airflow allows for complex dependencies between tasks. You can define workflows with conditional logic, retries, failure handling, and task ordering (e.g., “Task B only runs if Task A succeeds”).
- Its Directed Acyclic Graph (DAG) structure enables defining and visualizing multi-step pipelines, which can be challenging to manage with simple serverless functions.

#### 2. **Integration with Diverse Data Sources and Tools**
- Airflow has a rich ecosystem of operators and hooks that integrate with databases, cloud services, and APIs, making it versatile for different ETL scenarios. It can handle diverse sources, such as Dune API data, S3 storage, and other cloud resources, which simplifies building a complete ETL pipeline within a single framework.

#### 3. **Scheduling Flexibility**
- Airflow has robust scheduling capabilities, allowing tasks to run at specific intervals or be triggered by events. This scheduling flexibility is crucial for ETL jobs that must process data at regular intervals (e.g., hourly, daily, weekly) or in response to specific conditions.

#### 4. **Monitoring and Visibility**
- Airflow provides a powerful web-based interface that allows you to visualize the entire pipeline, monitor task progress, track execution status, and view logs. This visibility is essential for debugging, diagnosing failures, and understanding the flow of data through each step of the ETL process.
- Alerts and notifications can be configured in case of task failures, making it easier to respond to issues in real-time.

#### 5. *Scalability and Resource Management**
- As the ETL process scales, Airflow can handle increasing data volumes and complex pipelines by distributing workloads across a cluster. This makes it suitable for large-scale data processing, where multiple data sources or high-volume data are involved.
- Airflow’s ability to integrate with distributed computing frameworks like Spark or Dask also adds to its scalability for heavy workloads.
