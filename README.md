# Indian Logistics Data Engineering Pipeline

I built this project to work on a real-world data engineering use case around logistics and shipment data in India.

The idea was to take raw shipment data, process it through different stages, generate useful analytics and make the results available through a Power BI dashboard. I also wanted to explore how batch processing and real-time event processing work together in a data pipeline.

## What does this project do?

The pipeline works with 25,000 shipment records and processes them using PySpark. It follows a Bronze, Silver and Gold architecture.

- **Bronze:** Loads raw CSV data and stores it in Parquet format.
- **Silver:** Cleans the data, handles missing or inconsistent values and prepares it for analysis.
- **Gold:** Creates fact and dimension tables along with aggregated datasets for reporting.

Apart from batch processing, I also implemented a Kafka-based streaming component to generate and process simulated shipment events.

## Technologies Used

- Python
- Apache PySpark
- Apache Kafka
- Spark Structured Streaming
- Apache Hive
- Apache Airflow
- Docker and Docker Compose
- PostgreSQL
- Power BI
- Pytest

## How I built it

### 1. Batch Processing with PySpark

The pipeline starts with a logistics CSV dataset containing 25,000 records.

I used PySpark to ingest the data, clean it and transform it into structured Parquet datasets. The Silver layer includes data-quality checks, delivery-duration processing and additional derived fields.

### 2. Data Modeling

For the Gold layer, I created a star-schema model consisting of:

**Fact table**
- fact_shipments

**Dimension tables**
- dim_delivery_partner
- dim_region
- dim_vehicle
- dim_package_type

I also generated four analytical tables covering delivery partner KPIs, regional performance, weather impact and vehicle efficiency.

### 3. Real-Time Data Processing

I used Kafka to generate simulated shipment events and send them to a Kafka topic.

Spark Structured Streaming consumes these events, processes them in microbatches and writes the processed data to Parquet files. Checkpointing is configured to maintain streaming progress.

### 4. Hive and SQL Analytics

I integrated Spark with Hive support and registered the Gold tables in the `india_logistics_dw` database.

I also wrote seven SQL queries to analyse different aspects of the logistics data, including:

- Delivery partner performance
- Regional delivery bottlenecks
- Weather impact on delays
- Electric versus conventional vehicles
- Package delivery risk

### 5. Workflow Orchestration with Airflow

I created an Airflow DAG to automate the batch pipeline.

It contains nine tasks, starting with raw data validation and ending with analytical query execution. I also configured retries and task dependencies.

The Airflow batch tasks currently execute Spark locally using `local[2]`.

### 6. Power BI Dashboard

I connected the generated CSV datasets to Power BI and built an interactive dashboard.

It includes:

- Total shipment events
- Delayed events and delay rate
- Average delivery distance
- Average delivery rating
- Delivery partner and regional performance
- Weather impact on delays
- Vehicle-wise on-time delivery performance

## Results

Here are some results from the completed batch pipeline:

| Metric | Result |
|---|---:|
| Shipment records processed | 25,000 |
| Gold tables generated | 9 |
| Hive analytical queries | 7 |
| On-time delivery rate | 73.32% |
| Failed delivery rate | 5.31% |
| Average delivery duration | 6.25 hours |
| Average delivery cost | ₹864.94 |

The complete batch workflow was successfully executed through Airflow, including data transformation, Gold table generation and Hive analytical queries.

## Project Structure

```text
india-logistics-data-engineering-pipeline/
│
├── airflow/
│   └── dags/
├── config/
├── docker/
├── docs/
├── hive/
│   └── queries/
├── kafka/
├── powerbi/
│   └── data/
├── src/
│   ├── batch/
│   ├── ingestion/
│   ├── streaming/
│   ├── transformations/
│   └── utils/
├── tests/
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Running the Project

**Requirements:**
- Python 3.10
- Java 17
- Docker Desktop
- Power BI Desktop (for viewing the dashboard)

Clone the repository:

```bash
git clone https://github.com/Virat213/india-logistics-data-engineering-pipeline.git

cd india-logistics-data-engineering-pipeline
```

Create and activate a virtual environment:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

Install the dependencies:

```bash
pip install -r requirements.txt
```

Place the source dataset at:

```text
data/raw/Delivery_Logistics.csv
```

Run the batch pipeline:

```bash
python -m src.batch.run_batch_pipeline
```

To start the Docker services, first create your local environment file:

```powershell
Copy-Item .env.example .env
```

Update the credentials in `.env`, then run:

```bash
docker compose up -d --build
```

Airflow is available at:

`http://localhost:8085`

For additional setup instructions, refer to `docs/DEPLOYMENT_GUIDE.md`.

## A few things to note

- The streaming component uses simulated shipment events rather than a live logistics API.
- Airflow orchestrates the batch workflow using local Spark execution.
- Hive integration uses Spark's embedded Hive support.
- The raw source dataset and generated Parquet files are not included in the repository.

## What's next?

Some improvements I'd like to work on:

- Incremental data processing
- Event-time windowed streaming analytics
- Better pipeline monitoring and alerting
- Automated testing through GitHub Actions
- Cloud deployment

---

**Developed by Virat**

[GitHub Repository](https://github.com/Virat213/india-logistics-data-engineering-pipeline)