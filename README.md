# Cloud-Analytics-Engineering-Pipeline
End-to-end cloud analytics pipeline integrating MySQL on GCP, Snowflake data warehousing, Streamlit application development, and Tableau BI dashboards. Includes full data modeling (star schema), cloud-to-cloud migration using GCS + Snowflake external stages, and interactive analytics powered by Snowflake.

![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)
![GCP](https://img.shields.io/badge/Google%20Cloud-4285F4?logo=googlecloud&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-00618A?logo=mysql&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?logo=streamlit&logoColor=white)
![Tableau](https://img.shields.io/badge/Tableau-E97627?logo=tableau&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)

A full end-to-end data engineering and analytics workflow using MySQL, Google Cloud Platform (GCP), Snowflake, Streamlit, and Tableau.

---

## Overview

This project implements a complete analytics pipeline using a modified version of the Sakila database. The workflow includes:

- Data modeling (fact & dimension tables)  
- Cloud database hosting (MySQL on GCP)  
- Data migration from GCP → Snowflake  
- Storage integration & staged loading in Snowflake  
- Streamlit application for interactive analysis  
- Tableau dashboard connected live to Snowflake  

The goal is to transform transactional Sakila data into a scalable analytics platform.

---
```
## Architecture Summary

MySQL (GCP Cloud SQL)
        ↓
Fact & Dimension Table Creation (SQL)
        ↓
Export to GCS Bucket (via GCP Cloud Shell)
        ↓
Snowflake Storage Integration (GCS External Stage)
        ↓
COPY INTO Snowflake Staging Tables
        ↓
Snowflake Streamlit App (Python)
        ↓
Tableau Dashboard (Live Snowflake Connection)
```
---

## 1. MySQL Instance on GCP (Cloud SQL)

- A MySQL instance was created on **Google Cloud SQL**.  
- MySQL Workbench connected directly to this cloud instance.  
- All fact and dimension tables were created using DDL and DML on the GCP-hosted database.  
- This ensured the entire schema lived in the cloud before migration.

---

## 2. Migration to Snowflake Using GCP API + COPY INTO

### A. GCS Bucket Creation  
A Google Cloud Storage bucket was created to store exported tables.

### B. Cloud Shell Export  
MySQL tables were exported as CSVs directly to the GCS bucket via Cloud Shell.

### C. Snowflake Storage Integration  
Snowflake was granted secure access to the GCS bucket using a **storage integration object**.

### D. Staging Tables  
Snowflake staging tables mirrored the structure of the MySQL tables.

### E. COPY INTO Command  
Data was migrated using:

```
COPY INTO database.schema.table
FROM @gcs_stage/path/
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"');
```

This completed the cloud-to-cloud migration cleanly.

---

## 3. Streamlit App Built in Snowflake

- Developed using **Snowflake’s Native App Framework**  
- Built with Python + Streamlit  
- Allows live interactive analysis of the Sakila dataset  
- Fully powered by Snowflake’s compute layer  

The app enables drill-downs into customer behavior, rental patterns, film performance, and more.

---

## 4. Tableau Dashboard (Live Snowflake Connection)

- Tableau connected to Snowflake using the **native connector**  
- A **live connection** was used (no extracts)  
- Dashboards include:
  - Rental performance  
  - Inventory and store metrics  
  - Customer segmentation  
  - Film genre insights  

Because the connection is live, dashboards update automatically whenever the Snowflake tables refresh.

---

## Project Structure

```
EndtoEnd_Sakila-Analytics-Pipeline/
│
├── data modeling/
│ ├── adi_singh_SakilaSnowflakeDW-DDL.sql
│ ├── adi_singh_SakilaSnowflakeDW-DML.sql
│ └── ERD.png
│
├── gcp snowflake migration/
│ ├── export_script.rtf
│ └── snowflake_staging_and_copy_script.sql # copied script from snowflake and saved to sql file
│
├── snowflake streamlit app/
│ ├── streamlit_app.py
│ ├── streamlit_app_screenshots.docx
│ └── requirements.rtf
│
├── tableau dashboard/
│ ├── tableau_dashboard_screenshots.docx
│ └── adi_singh_snowflake.twb
│
└── README.md
```

---

## Key Skills Demonstrated

### Data Engineering  
- Cloud SQL database creation  
- GCS bucket management  
- ETL pipeline: Cloud Shell → GCS → Snowflake  
- COPY INTO from external stage  
- Snowflake storage integration  

### Analytics Engineering  
- Star schema design  
- Fact/dimension modeling  
- Documentation & reproducible SQL workflows  

### Application & BI Development  
- Snowflake Streamlit app  
- Tableau live-connected dashboards  

### Cloud Technologies  
- Google Cloud Platform (Cloud SQL, GCS, Cloud Shell)  
- Snowflake Data Cloud  
