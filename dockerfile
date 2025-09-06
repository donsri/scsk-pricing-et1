FROM apache/airflow:2.10.0

# Switch to root to install system dependencies
USER root

# Install system dependencies
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements and install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy your project files
COPY . /opt/airflow/project/

# Set working directory
WORKDIR /opt/airflow