# IPL Analytics Pipeline

An end-to-end Data Engineering project built following industry-standard **Medallion Architecture** (Bronze → Silver → Gold), processing **283,678 ball-by-ball IPL records** from 2008 to 2025.

---

## System Architecture

```

```

---

## Tech Stack

| Layer | Tool | Version |
|---|---|---|
| Ingestion | Python + Kaggle API | 3.11 |
| Data Lake | Amazon S3 | - |
| Processing | PySpark | 3.5.0 |
| Warehouse | PostgreSQL | 15 |
| Transformation | dbt | 1.7.0 |
| Orchestration | Apache Airflow | 2.8.0 |
| Monitoring | Grafana | Latest |
| Containerization | Docker | - |

---

## Project Structure

```
ipl-analytics-pipeline/
│
├── dags/
│   └── ipl_pipeline.py               # Airflow DAG
│
├── ingestion/
│   └── kaggle_to_s3.py               # Kaggle to S3 Bronze
│
├── spark_jobs/
│   └── bronze_to_silver.py           # PySpark transformation
│
├── dbt/
│   └── ipl_analytics/
│       ├── models/
│       │   ├── staging/
│       │   │   └── stg_ipl_matches.sql
│       │   └── marts/
│       │       ├── top_batsmen.sql
│       │       ├── top_bowlers.sql
│       │       ├── team_performance.sql
│       │       └── season_trends.sql
│       ├── tests/
│       │   └── assert_no_null_match_id.sql
│       └── dbt_project.yml
│
├── monitoring/
│   └── grafana/
│       └── dashboard.json
│
├── Dockerfile.spark
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

---

## Prerequisites

- Docker Desktop installed and running
- Python 3.11+
- AWS Account (free tier is sufficient)
- Kaggle Account
- Git

---

## Setup Guide

### Step 1 — Clone the Repository

```bash
git clone https://github.com/darshanPC25/ipl-analytics-pipeline.git
cd ipl-analytics-pipeline
```

### Step 2 — Create Environment File

```bash
cp .env.example .env
```

Open `.env` and fill in your credentials:

```bash
# AWS Credentials
# Get from: AWS Console -> IAM -> Users -> Security Credentials -> Create Access Key
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_REGION=ap-south-1

# S3 Buckets
# Create these buckets in AWS S3 Console
S3_BRONZE_BUCKET=yourname-s3-ipldata-bronze-dev
S3_SILVER_BUCKET=yourname-s3-ipldata-silver-dev
S3_GOLD_BUCKET=yourname-s3-ipldata-gold-dev

# PostgreSQL
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_PORT_LOCAL=5433
POSTGRES_DB=ipl_warehouse
POSTGRES_USER=ipl_user
POSTGRES_PASSWORD=ipl_password_123

# Kaggle
# Get from: kaggle.com -> Profile -> Settings -> API -> Create New Token
KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_api_key

# Airflow
AIRFLOW_UID=50000
```

### Step 3 — Create AWS S3 Buckets

Go to AWS S3 Console and create these 3 buckets in `ap-south-1` (Mumbai):

```
yourname-s3-ipldata-bronze-dev   (versioning: ON)
yourname-s3-ipldata-silver-dev   (versioning: OFF)
yourname-s3-ipldata-gold-dev     (versioning: OFF)
```

Settings for all buckets:
```
ACLs              -> Disabled
Block all public  -> ON
Encryption        -> SSE-S3
```

### Step 4 — Create IAM User

Go to AWS IAM Console and create a user:

```
Username   -> yourname-ipl-local
Permission -> AmazonS3FullAccess
```

Generate access key and add to `.env` file.

### Step 5 — Setup Kaggle Credentials

```bash
mkdir C:\Users\YourUsername\.kaggle
```

Create `kaggle.json`:

```json
{
    "username": "your_kaggle_username",
    "key": "your_kaggle_api_key"
}
```

### Step 6 — Start Docker Stack

```bash
docker-compose up -d
```

Wait 2-3 minutes for all containers to start.

Verify all containers are running:

```bash
docker-compose ps
```

You should see:

```
postgres    running
airflow     running
grafana     running
pyspark     running
```

### Step 7 — Create PostgreSQL Schemas

```bash
docker exec -it <postgres-container-name> psql -U ipl_user -d ipl_warehouse -c "
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
"
```

### Step 8 — Setup dbt

Install dbt:

```bash
pip install dbt-postgres
```

Configure `~/.dbt/profiles.yml`:

```yaml
ipl_analytics:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5433
      user: ipl_user
      pass: ipl_password_123
      dbname: ipl_warehouse
      schema: gold
      threads: 4
```

Test connection:

```bash
cd dbt/ipl_analytics
dbt debug
```

You should see:
```
Connection test: OK
All checks passed!
```

### Step 9 — Install Python Dependencies

```bash
pip install -r requirements.txt
```

---

## Running the Pipeline

### Manual Run

Run each step individually:

```bash
# Step 1 - Ingest from Kaggle to S3 Bronze
python ingestion/kaggle_to_s3.py

# Step 2 - PySpark Bronze to Silver
docker exec -it <pyspark-container-name> /bin/bash -c "cd /opt/spark_jobs && python3 bronze_to_silver.py"

# Step 3 - dbt Gold models
cd dbt/ipl_analytics
dbt run
```

### Automated Run via Airflow

```
1. Open http://localhost:8080
2. Username: admin / Password: admin
3. Find ipl_analytics_pipeline DAG
4. Enable the toggle
5. Click Trigger DAG
```

Pipeline runs automatically daily at 6AM.

---

## Accessing Services

| Service | URL | Username | Password |
|---|---|---|---|
| Airflow | http://localhost:8080 | admin | admin |
| Grafana | http://localhost:3000 | admin | admin |
| PostgreSQL | localhost:5433 | ipl_user | ipl_password_123 |

---

## Key Metrics

- **283,678** ball-by-ball records processed
- **16+ IPL seasons** covered (2008-2025)
- **4 Gold layer** business models
- **Daily automated** pipeline via Airflow
- **3-layer** Medallion Architecture

---

## Sample Results

### Top Batsmen (By Season)

| Batsman | Total Runs | Strike Rate | Season |
|---|---|---|---|
| V Kohli | 973 | 148.55 | 2016 |
| Shubman Gill | 890 | 152.92 | 2023 |
| JC Buttler | 863 | 144.80 | 2022 |
| DA Warner | 848 | 146.46 | 2016 |

---

## Troubleshooting

### Port Conflict with Local PostgreSQL

If you have PostgreSQL installed locally, it may occupy port 5432. The docker-compose maps port 5433 externally to avoid this conflict.

```
Inside Docker  -> postgres:5432
Outside Docker -> localhost:5433
```

### Airflow Not Starting

Check logs:

```bash
docker-compose logs airflow
```

If DB not initialized, restart:

```bash
docker-compose down -v
docker-compose up -d
```

### PySpark Cannot Connect to S3

Verify environment variables are loaded inside container:

```bash
docker exec -it <pyspark-container> /bin/bash -c "echo $S3_BRONZE_BUCKET"
```

If empty, ensure `env_file` is set in docker-compose.yml for pyspark service.

---

## Author

**Paridarshan Sahoo**
- GitHub: [github.com/darshanPC25](https://github.com/darshanPC25)
- LinkedIn: [linkedin.com/in/darshanpc](https://linkedin.com/in/darshanpc)
- Email: goodparidarshan@gmail.com