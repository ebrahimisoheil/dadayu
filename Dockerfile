FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /opt/dagster/home
ENV DAGSTER_HOME=/opt/dagster/home

COPY . .

RUN pip install --no-cache-dir -r warehouse/requirements-dbt.txt && \
    cd /app/warehouse && dbt deps

CMD ["dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4000", "-m", "dagster_pipeline.definitions"]
