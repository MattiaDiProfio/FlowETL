FROM apache/airflow:2.10.4

USER root

# install system dependencies if needed
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# install Python dependencies
RUN pip install --no-cache-dir \
    kafka-python-ng \
    # thefuzz \
    # sentence-transformers \
    # openai