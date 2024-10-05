# Use the base image you are currently using (replace with your base image if different)
FROM apache/airflow:2.9.3-python3.11

# Copy the requirements.txt file to the working directory
COPY requirements.txt .

# Upgrade pip and install the required Python packages
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install "apache-airflow==2.9.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"

