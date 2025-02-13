
# STEDI Human Balance Analytics

## **Project Overview**
This project focuses on building a **data lakehouse solution** for the **STEDI Step Trainer**, a device that helps users improve their balance. The solution processes sensor data from the **Step Trainer** and **companion mobile app** to create a curated dataset for **machine learning**.

The solution uses **AWS Glue**, **AWS Athena**, and **Amazon S3** to:
- Ingest, clean, and transform the raw data.
- Prepare the data for machine learning model training.

## **Technologies Used**
- **AWS Glue**: Used to process and clean the data with Glue jobs.
- **Amazon S3**: Used for storing the raw and curated data.
- **AWS Athena**: Used for querying the data.

---

## **Project Details**
The project involved processing the following data sources:

- **Step Trainer Data**: Data from a motion sensor on the device.
- **Accelerometer Data**: Data collected by the mobile app from the user’s phone.
- **Customer Data**: Customer information related to the STEDI device.

### **Data Transformation Pipeline**

1. **Landing Zone**: Raw data from S3 (i.e., `accelerometer_landing`, `step_trainer_landing`, and `customer_landing`).
2. **Trusted Zone**: Cleaned data for trusted customers and accelerometer data (i.e., `accelerometer_trusted_data`, `customer_trusted_data`, and `step_trainer_trusted_data`).
3. **Curated Zone**: Curated data for machine learning (i.e., `customer_curated_data`, `machine_learning_curated_data`).

---

## **How It Works**
### **1. Data Ingestion**
Data is ingested from **Amazon S3** into **AWS Glue**. Three main landing zones are used to store the raw data:
- **Accelerometer Data** (`s3://stedi-project-bucket/accelerometer_landing/`)
- **Step Trainer Data** (`s3://stedi-project-bucket/step_trainer_landing/`)
- **Customer Data** (`s3://stedi-project-bucket/customer_landing/`)

### **2. Data Cleaning and Filtering (AWS Glue Jobs)**
- **Filter Customer Data**: Customers who agreed to share their data for research are filtered.
- **Join Data**: The accelerometer and step trainer data are joined with customer data based on customer IDs (email or serial numbers).
- **Data Transformation**: Data is transformed using **AWS Glue Jobs** in Python, specifically by using **DynamicFrames** and **DataFrames** for cleaning and filtering operations.

### **3. Data Storage**
- Cleaned and filtered data is written to **Amazon S3** in different folders for each zone:
  - **Customer Trusted Data**: `s3://stedi-project-bucket/customer_trusted_data/`
  - **Accelerometer Trusted Data**: `s3://stedi-project-bucket/accelerometer_trusted_data/`
  - **Step Trainer Trusted Data**: `s3://stedi-project-bucket/step_trainer_trusted_data/`
  - **Customer Curated Data**: `s3://stedi-project-bucket/customer_curated_data/`
  - **Machine Learning Curated Data**: `s3://stedi-project-bucket/machine_learning_curated_data/`

### **4. Data Querying (AWS Athena)**
- **Athena** is used to run SQL queries on the S3-stored data.
- Queries help verify data correctness and row counts, ensuring the integrity of the pipeline.

---

## **Scripts**
All scripts used for transforming and processing the data are located in the `scripts/` directory. The key Glue jobs include:

- **`customer_landing_to_trusted.py`**: Filters and writes customer data to the trusted zone.
- **`accelerometer_landing_to_trusted.py`**: Filters and writes accelerometer data to the trusted zone.
- **`step_trainer_landing_to_trusted.py`**: Filters and writes step trainer data to the trusted zone.
- **`customer_trusted_to_curated.py`**: Joins customer data with accelerometer data to create curated customer data.
- **`machine_learning_curated.py`**: Joins step trainer and accelerometer data to prepare the final dataset for machine learning.

---

## **Data Overview**
The data in each zone is structured as follows:

1. **Customer Landing Data**  
   - `customerName`: string  
   - `email`: string  
   - `phone`: string  
   - `birthDay`: string  
   - `serialNumber`: string  
   - `registrationDate`: bigint  
   - `lastUpdateDate`: bigint  
   - `shareWithResearchAsOfDate`: bigint  
   - `shareWithPublicAsOfDate`: bigint  
   - `shareWithFriendsAsOfDate`: bigint  

2. **Accelerometer Landing Data**  
   - `user`: string  
   - `timestamp`: bigint  
   - `x`: double  
   - `y`: double  
   - `z`: double  

3. **Step Trainer Landing Data**  
   - `sensorReadingTime`: bigint  
   - `serialNumber`: string  
   - `distanceFromObject`: double  

---

## **Project Submission**
This repository includes the following:
1. **Python Glue Scripts**: Located in the `scripts/` directory.
2. **Screenshots**: Located in the `screenshots/` directory.
3. **Query Results**: Screenshots for all key queries executed in **AWS Athena**.

---

## **Conclusion**
This project provides a solid foundation for building an **ETL pipeline** for sensor data, cleaning and preparing it for machine learning. By using **AWS Glue**, **Athena**, and **S3**, the project efficiently processes large datasets and generates valuable insights for STEDI’s human balance analytics.



Trusted Zone: Cleaned data containing only customers who consented to share their data.
Curated Zone: Data ready for machine learning, combining accelerometer and step trainer data.
