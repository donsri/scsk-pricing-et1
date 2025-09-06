**SCSK Pricing ETL Pipeline**
A comprehensive Docker-based ETL pipeline that extracts product data from an external API, transforms it with currency conversion and business logic, and loads it into Azure SQL Database using Apache Airflow orchestration.

**Architecture Overview**
API → Azure Blob Storage → Data Transformation → Azure SQL Database
     (Raw Data)           (Processed Data)      (Final Tables)

**Project Structure**
scsk-pricing-etl/
├── README.md
├── docker-compose.yml          # Docker orchestration
├── Dockerfile                  # Airflow container definition
├── requirements.txt            # Python dependencies
├── .env                        # Environment variables (not in repo)
├── .gitignore
├── dags/                       # Airflow DAG definitions
│   └── docker_pricing_etl_dag.py
├── etl/                        # ETL pipeline modules
│   ├── __init__.py
│   ├── config.py               # Configuration management
│   ├── utils.py                # Utility functions
│   ├── ingest.py               # Data ingestion from API
│   ├── transform.py            # Data transformation logic
│   └── load.py                 # Data loading to SQL
├── sql/                        # Database schema definitions
│   ├── create_tables.sql       # Table creation scripts
│   └── indexes.sql             # Index definitions
├── logs/                       # Airflow logs (auto-generated)
├── plugins/                    # Airflow plugins (auto-generated)
└── tests/                      # Unit tests
    ├── test_ingest.py
    ├── test_transform.py
    └── test_load.py

**Features**

Containerized Architecture: Full Docker setup with Apache Airflow
Data Lake Integration: Azure Blob Storage for raw and processed data
Currency Conversion: Real-time GBP conversion with exchange rate tracking
Business Logic: Product categorization and price band analysis
Error Handling: Comprehensive logging and retry mechanisms
Monitoring: Airflow web UI for pipeline monitoring and debugging

**Prerequisites**

Docker and Docker Compose
Azure Storage Account
Azure SQL Database
Exchange rate API access (exchangerate-api.com)

**Environment Setup**

Clone the repository:
bashgit clone <repository-url>
cd scsk-pricing-etl

Create environment file:
bashcp .env.example .env

Configure environment variables in .env:
bash# API Configuration
API_BASE_URL=https://fakestoreapi.com
EXCHANGE_RATE_API_KEY=your_api_key_here

# Azure Storage
AZURE_STORAGE_ACCOUNT_NAME=your_storage_account
AZURE_STORAGE_CONTAINER=datalake
AZURE_STORAGE_SAS=your_sas_token

# Azure SQL Database
AZURE_SQL_SERVER=your-server.database.windows.net
AZURE_SQL_DATABASE=your_database
AZURE_SQL_USERNAME=your_username
AZURE_SQL_PASSWORD=your_password

# Pipeline Settings
PRICE_EXPENSIVE_THRESHOLD_GBP=100


**Installation & Deployment**

Create required directories:
bashmkdir -p dags logs plugins
chmod 755 dags logs plugins

Initialize Airflow database:
bashdocker-compose up airflow-init

Start all services:
bashdocker-compose up -d

Access Airflow Web UI:

URL: http://localhost:8081
Username: airflow
Password: airflow



**Pipeline Components**
1. Data Ingestion (etl/ingest.py)

Fetches product data from FakeStore API
Stores raw JSON data in Azure Blob Storage
Implements retry logic and error handling

2. Data Transformation (etl/transform.py)

Converts USD prices to GBP using live exchange rates
Adds business logic (expensive flag, price bands)
Enriches data with processing metadata
Outputs processed Parquet files

3. Data Loading (etl/load.py)

Creates/manages SQL Server tables
Loads processed data into Azure SQL Database
Implements data validation and verification

4. Orchestration (dags/docker_pricing_etl_dag.py)

Airflow DAG with 4 tasks: ingest → transform → load → summary
Configurable scheduling and retry policies
Comprehensive logging and monitoring

**Database Schema**
The pipeline creates the following table structure:
sqlCREATE TABLE dbo.products (
    product_id INT PRIMARY KEY,
    title NVARCHAR(255),
    price_usd DECIMAL(10,2),
    price_gbp DECIMAL(10,2),
    description NVARCHAR(MAX),
    category_name NVARCHAR(100),
    rating DECIMAL(3,2),
    rating_count INT,
    expensive BIT,
    price_band NVARCHAR(20),
    processing_date DATE,
    exchange_rate_used DECIMAL(10,6),
    ingested_at DATETIME2,
    created_at DATETIME2 DEFAULT GETDATE()
);

Running the Pipeline

Manual Execution

Access Airflow UI at http://localhost:8081
Navigate to DAGs → docker_pricing_etl_pipeline
Click "Trigger DAG" to run manually

Scheduled Execution
The pipeline is configured to run daily at midnight UTC. Modify the schedule in docker_pricing_etl_dag.py:
pythonschedule_interval='0 0 * * *',  # Daily at midnight
Monitoring & Logs

Airflow UI: Real-time task status and logs
Docker Logs: docker-compose logs -f
Task Logs: Available through Airflow UI

Data Flow

Raw Data: raw/products/date=YYYY-MM-DD/products_YYYYMMDD_HHMMSS.json
Processed Data: processed/products/date=YYYY-MM-DD/products_YYYYMMDD_HHMMSS.parquet
Database: dbo.products table in Azure SQL

Development
Running Tests
bash# Unit tests
python -m pytest tests/

# Integration tests
docker exec -it scsk-pricing-etl-airflow-webserver-1 python -c "from etl.load import test_sql_connection; test_sql_connection()"
Adding New Features

Create feature branch: git checkout -b feature/new-feature
Update relevant ETL modules in etl/ directory
Add/update tests in tests/ directory
Update documentation and commit changes

Troubleshooting
Common Issues

Connection Timeout to Azure SQL:

Verify firewall rules in Azure Portal
Check connection string in logs
Ensure Docker containers can access external networks


Azure Blob Storage Access:

Verify SAS token permissions and expiry
Check storage account name and container


Pipeline Task Failures:

Check Airflow task logs via web UI
Verify environment variables are set correctly
Review Docker container logs



Useful Commands
bash# Restart services
docker-compose restart

# View logs
docker-compose logs -f airflow-webserver
docker-compose logs -f airflow-scheduler

# Access container shell
docker exec -it scsk-pricing-etl-airflow-webserver-1 bash

# Stop all services
docker-compose down -v
Production Considerations

Security: Use Azure Key Vault for sensitive configuration
Scaling: Consider using Kubernetes for production deployment
Monitoring: Implement alerting for pipeline failures
Backup: Regular database and blob storage backups
Performance: Monitor and optimize SQL queries and data processing