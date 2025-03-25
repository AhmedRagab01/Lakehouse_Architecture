# Lakehouse Architecture - AWS Glue

## Project Overview
This project is designed to create a data lakehouse solution using **AWS Glue, Amazon S3, Athena, and Apache Spark** to process and curate sensor data for training a **machine learning model**. The model will be used to detect steps in real-time based on motion sensor data collected from the STEDI Step Trainer and mobile phone accelerometers.

## Project Workflow
1. **Landing Zone Creation**
   - Store raw sensor data in S3.
   - Three key data sources:
     - `customer_landing`
     - `step_trainer_landing`
     - `accelerometer_landing`

2. **Glue Tables for Landing Zone**
   - Created Glue tables for initial raw data storage.
   - Queried tables using **Amazon Athena**.

3. **Data Transformation Using AWS Glue Jobs**
   - **Sanitized Customer Data** → `customer_trusted`
   - **Filtered Accelerometer Data** → `accelerometer_trusted`
   - **Curated Customer Data** (Only those with accelerometer data) → `customers_curated`

4. **Creating Trusted & Curated Tables**
   - **Step Trainer Trusted Data** → `step_trainer_trusted`
   - **Machine Learning Curated Data** → `machine_learning_curated`

## AWS Services Used
- **Amazon S3** - Data lake storage.
- **AWS Glue** - Data processing and ETL jobs.
- **Amazon Athena** - Querying curated datasets.
- **Amazon Redshift** (Optional) - Data warehousing.
- **AWS IAM** - Security & permissions management.

## Repository Structure
```
📂 Lakehouse_Architecture
├── 📂 DDL_Queries
│   ├── customer_landing.sql
│   ├── accelerometer_landing.sql
│   ├── step_trainer_landing.sql
├── 📂 Glue_Spark_Jobs
│   ├── customer_trusted.py
│   ├── accelerometer_trusted.py
│   ├── customers_curated.py
│   ├── step_trainer_trusted.py
│   ├── machine_learning_curated.py
├── README.md
```

## Setup Instructions
### 1. Clone Repository
```sh
git clone <repo_link>
cd project path
```

### 2. Configure AWS Credentials
Ensure your AWS credentials are set up:
```sh
aws configure
```

### 3. Create S3 Buckets & Upload Data
Create necessary S3 buckets and upload dataset files.

### 4. Create Glue Tables Using SQL Scripts
Run the SQL scripts in Athena to create initial Glue tables.

### 5. Run AWS Glue ETL Jobs
Execute the Glue jobs in AWS Glue Studio or using Python:
```sh
python Glue_Spark_Jobs/customer_trusted.py
```

### 6. Query Curated Data in Athena
Run queries in **Amazon Athena** to verify transformations.



## Conclusion
This project showcases an **end-to-end data engineering pipeline** using AWS Glue, S3, and Athena, enabling machine learning model training with curated sensor data. The solution ensures privacy by filtering only customers who agreed to share their data while maintaining high data quality.


