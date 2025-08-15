# A Real-Time Data Pipeline for Logistics Optimization

## 1. Executive Summary

This project documents the design and implementation of a high-frequency data engineering pipeline for optimizing the logistics of a perishable goods warehouse. Using a flower distribution center as a model, this system provides an end-to-end solution for ingesting event-driven data, applying business logic for inventory management, and serving real-time operational insights. The architecture is fully containerized, ensuring reproducibility and scalability.

### 1.1. The Business Problem

The effective management of perishable goods presents significant data-handling challenges. The core problem is one of managing a high-velocity stream of operational data (shipments, orders) against a stateful inventory with strict business rules (e.g., temperature constraints, shelf life). A failure to process this data reliably and in near real-time leads to direct financial loss through product spoilage and inefficient order fulfillment.

### 1.2. The Engineering Solution

This project implements an automated, event-driven system that processes micro-batches of data to maintain an accurate, real-time state of the warehouse. It decouples data generation, processing, and visualization into distinct services orchestrated by Docker Compose. Apache Airflow is utilized for its robust scheduling and dependency management capabilities, ensuring the reliable execution of the core data transformation logic. The result is a resilient and scalable data pipeline that translates raw operational events into actionable business intelligence.

---

## 2. Data Pipeline Architecture

The system is designed as a series of decoupled services communicating through a shared data volume and a central PostgreSQL database. This architecture ensures modularity and fault tolerance.

<img width="1629" height="705" alt="image" src="https://github.com/user-attachments/assets/c7bc7309-8de5-48c9-9729-0cd737bf5c41" />

### 2.1. Data Flow

1.  **Data Ingestion**: A standalone Python service simulates operational events, writing new shipment and order records to CSV files in a shared volume. This represents the raw, unstructured data source.
2.  **ELT Stage**: The Airflow-managed pipeline begins by loading the raw CSV data into staging tables within the PostgreSQL database. This approach ensures that the raw data is captured quickly and durably before any transformations occur.
3.  **ETL Stage (Transformation)**: The core logic reads from the staging tables and the production `inventory` table. It applies business rules for storage allocation and order fulfillment.
4.  **Data Serving**: The results of the transformation—updated inventory state and newly created operational tasks—are written back to production tables in the database. A Streamlit dashboard then queries these tables to provide a real-time visualization layer for end-users.

---

## 3. Optimization Logic

The core of the transformation stage involves two distinct optimization algorithms designed to maximize efficiency and minimize product loss.

### 3.1. Inbound Logistics: Optimal Storage Allocation

When a new shipment arrives, the system determines the ideal storage location by applying a multi-factor algorithm:

1.  **Temperature Matching**: The system first identifies all available storage bins that match the specific temperature requirement of the incoming flower type, as defined in the `flower_catalog.csv`.
2.  **Distance Minimization**: From this subset of suitable bins, the algorithm selects the one with the shortest physical distance to the packing area. This minimizes the travel time for warehouse workers during both storage and retrieval, optimizing labor efficiency.
3.  **Task Generation**: Once the optimal bin is chosen, a `STORAGE` task is generated and logged in the `tasks` table with precise instructions (e.g., "Store 150 Rose in Bin A01").

### 3.2. Outbound Logistics: FIFO Order Fulfillment

When a new customer order is processed, the system fulfills it using a First-In, First-Out (FIFO) strategy to ensure product freshness:

1.  **Inventory Check**: The system first verifies that there is sufficient quantity of the requested flower type across the entire warehouse.
2.  **FIFO Sorting**: It retrieves all stock batches of the required flower and sorts them by their `arrival_date` in ascending order, ensuring the oldest inventory is selected first.
3.  **Batch Picking**: The system fulfills the order by creating `PICKING` tasks, drawing from the oldest batches until the required quantity is met. If an order quantity exceeds a single batch, the system will intelligently generate multiple picking tasks across several bins.
4.  **Inventory Update**: The inventory table is updated in real-time to reflect the decremented quantities, ensuring the next pipeline run operates on an accurate state.

---


https://github.com/user-attachments/assets/71cffb1a-f88a-4443-a591-f30a50f7c69f



## 4. Technical Features

* **High-Frequency Batch Processing**: The Airflow DAG is configured to run every 30 seconds, processing data in small, manageable micro-batches to achieve near real-time state updates.
* **Stateful Inventory Management**: The pipeline is designed to be stateful, reading the current inventory from the database in each run to make informed decisions about new data.
* **Idempotent Operations**: The use of staging tables and transactional updates ensures that pipeline runs are idempotent and resilient to failure.
* **Decoupled Services**: Each component of the system (data generation, scheduling, processing, database, UI) runs in its own Docker container, allowing for independent scaling and maintenance.
* **Centralized Orchestration**: Apache Airflow serves as the single point of control for scheduling, monitoring, and logging all data processing tasks.

---

## 5. Technology Stack

* **Orchestration**: Apache Airflow
* **Containerization**: Docker & Docker Compose
* **Database**: PostgreSQL
* **Dashboard**: Streamlit
* **Core Language**: Python
* **Data Manipulation**: Pandas

---

## 6. System Deployment Guide

### 6.1. Prerequisites

* Docker Desktop
* Docker Compose (included with Docker Desktop)

### 6.2. Installation Steps

1.  **Clone the Repository**
    ```bash
    git clone [https://github.com/your-username/flower-warehouse-optimizer.git](https://github.com/your-username/flower-warehouse-optimizer.git)
    cd flower-warehouse-optimizer
    ```
2.  **Configure Environment Variables**
    Create a file named `.env` in the project root with the following content:
    ```env
    AIRFLOW_UID=50000
    AIRFLOW_GID=0
    ```
3.  **Build and Launch the Services**
    Execute the following command from the project root:
    ```bash
    docker-compose up --build
    ```

### 6.3. One-Time System Initialization

Upon the first launch, the database schema must be initialized.

1.  **Access the Airflow UI**: Navigate to `http://localhost:8080`.
2.  **Enable the DAG**: Activate the `flower_warehouse_optimizer_dag`.
3.  **Execute the Initialization Task**: Manually trigger the `initialize_database_schema` task from the Airflow UI to create the required tables.

The system is now fully operational.

---

## 7. Accessing System Interfaces

* **Real-Time Dashboard**: `http://localhost:8501`
* **Airflow Web UI**: `http://localhost:8080` (Default Credentials: `admin` / `admin`)

---

## 8. Repository Structure

```
/flower-warehouse-optimizer
|
|-- dags/
|   |-- warehouse_optimization_dag.py  # Airflow DAG definition
|
|-- data/
|   |-- flower_catalog.csv             # Static data for flower types
|   |-- warehouse_layout.csv           # Static data for bin locations
|
|-- scripts/
|   |-- app.py                         # Streamlit dashboard source code
|   |-- data_generator.py              # Data simulation service logic
|   |-- db_init.py                     # One-time database setup script
|   |-- run_pipeline.py                # Core ETL processing logic
|
|-- .env                               # Environment variables for Docker
|-- .gitignore                         # Specifies files for Git to ignore
|-- docker-compose.yml                 # Defines and orchestrates all services
|-- Dockerfile                         # Blueprint for the custom Docker image
|-- README.md                          # Project documentation
