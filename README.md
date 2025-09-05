**Project Title: SCSK Pricing ETL Pipeline**

**
Project Scenario**

This project is a small data pipeline that ingests product pricing data from a public API, transforms it, and makes it available for downstream analytics.


**Deliverables**

* Python scripts for the ETL process.
* An Airflow DAG file for orchestration.
* A SQL schema (DDL) file. (Yet to add)
* A brief  README.md with setup instructions, assumptions, and potential improvements.


Tech StackPython: For data extraction and transformation.
SQL: For database schema and data loading.Airflow: For orchestrating the data pipeline.Git: For version control.API: Fake Store API was used for product data and Exchange Rate API for currency conversion.

**Project Requirements**

The pipeline is designed to perform the following four key steps:
1. Ingest: Extract product data from a public API and save the raw data in a data lake format (e.g., Parquet or JSON).
2. Transform: Clean and normalize the data by converting prices to GBP using a separate API for exchange rates. A new column for the price in GBP is added.
3. Store: Perform basic feature engineering (e.g., categorizing product types or flagging items over a certain price threshold). The cleaned data is then loaded into a SQL-based database.
4. Orchestrate: An Airflow DAG is defined to orchestrate the tasks: ingest_data, transform_data, and load_to_db.

**Setup Instructions**

Follow these steps to set up and run the ETL pipeline locally:
1. Clone the Repository:

git clone https://github.com/donsri/scsk-pricing-et1.git
cd scsk-pricing-et1

2.Set up the Airflow Environment: You need to have Airflow installed and configured. If you are using a local setup, ensure you have the required dependencies from the requirements.txt file. Your project structure should look similar to this:
3.Update .gitignore: Ensure your .gitignore file includes sensitive files to prevent them from being committed to the repository, such as airflow.cfg, airflow.db, and .env.

4.Start Airflow Components: Open two separate terminals. In the first terminal, start the Airflow scheduler: airflow scheduler
In the second terminal, start the Airflow web server:airflow webserver -p 8080

5.Access the Airflow UI: Open your web browser and navigate to http://localhost:8080 to access the Airflow UI .
6.Place the DAG: Place the scsk_pricing_etl_dag.py file into the dags/ folder of your Airflow directory. Airflow will automatically detect and load the DAG.
7.Run the Pipeline:
* In the Airflow UI, find the scsk_pricing_etl_pipeline DAG in the list .
* Manually trigger the DAG by clicking the "Trigger DAG" button.
* Monitor the progress of the ingest_data, transform_data, load_data, and pipeline_summary tasks in the Graph or Gantt views .

**Assumptions Made**

* The pipeline uses the Fake Store API as the data source for product pricing.
* The Exchange Rate.host API is used to convert prices to GBP.
* For a local setup, the SequentialExecutor is used, and a SQLite database acts as the metadata store.

**
What Would Be Improved with More Time?**

* Robustness: Implement more comprehensive data quality checks and validation steps within the pipeline to handle missing or corrupt data.
* Production Readiness:
    * Migrate from the SQLite database to a more scalable solution like PostgreSQL or MySQL for the Airflow metadata store to enable parallel processing.
    * Use a more robust executor like LocalExecutor or CeleryExecutor.
    * Externalize credentials and secrets using Airflow's Secrets Backend instead of storing them in the code.
* Modularity: Further refactor the code to improve modularity and reusability, such as creating custom operators for the ETL tasks.
