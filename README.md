# Introduction 
This project showcases a CI/CD pipeline framework developed for ELT (Extract, Load, Transform) processes, tailored to client-specific data sources and business requirements. It automates data ingestion, transformation, and deployment using modern data engineering tools and cloud services.

# Technologies Used

Snowflake:
Stores raw and processed data.
Maintains historical versions of records.
Uses streams, tasks, and procedures to move data from staging (variant tables) to target tables.
Automatically generates DDL scripts for Snowflake objects.
(Sample scripts available in the Dumps zip file)

PySpark:
Core framework used for building the data pipeline logic and transformations.

Apache Airflow:
Orchestrates jobs to extract data from client sources and load it into Snowflake.
Jobs are scheduled based on business requirements.
Logs all job activity including info, warnings, and errors.

Azure DevOps:
Manages source control and CI/CD pipelines.
Builds and deploys the latest code to production environments.

SQL Server:
Stores configuration metadata for each pipeline.
Maintains logs and history of processed files.
Hosts client data that is replicated to Snowflake using COPY commands.
(Sample queries available in the Dumps zip file)

# Build and Test

WinSCP: Used for secure file transfers and code updates.
Saviynt Cloud: Generates secure passwords for deployment and access.
Visual Studio: Development environment for framework components.
Azure DevOps: Used for testing and deploying the latest code versions via service connections.

