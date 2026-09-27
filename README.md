# Ride-Hailing Data Engineering Platform

An end-to-end real-time and batch data engineering platform for ride-hailing fleet operations.

## Project Overview

This project implements a complete data pipeline for monitoring ride-hailing fleet activity in real time while reconciling fleet performance against daily vehicle expenses.

The platform simulates:

- Continuous vehicle GPS and trip telemetry
- Daily vehicle fuel and maintenance expense feeds
- Real-time stream processing
- Daily reconciliation
- Fleet utilization analytics
- Vehicle profitability analysis
- Operational alerts
- Monitoring and observability

## Architecture

The project uses a Kappa-oriented streaming architecture with Apache Kafka, Apache Spark Structured Streaming, Apache Airflow, PostgreSQL, FastAPI, and monitoring components.

## Technology Stack

- Python
- Apache Kafka
- Apache Spark Structured Streaming
- Apache Airflow
- PostgreSQL
- FastAPI
- Prometheus
- Grafana
- Docker / Docker Compose
- Pytest

## Main Business Questions

1. What is the current fleet utilization?
2. How many vehicles are active or idle?
3. What are the current trips and earnings by zone?
4. Which vehicles are becoming unprofitable?
5. How do daily fuel and maintenance costs affect vehicle profitability?

## Simulated Time

For demonstration purposes, one simulated business day is compressed into a short period so that the complete pipeline can be demonstrated within the assessment session.

The exact simulation configuration is documented in the project report.

## Project Structure

```text
producers/       Simulated streaming and batch data sources
spark/           Spark Structured Streaming processing
airflow/         Airflow orchestration
api/             FastAPI serving layer
database/        PostgreSQL initialization
monitoring/      Prometheus and Grafana configuration
tests/           Automated tests
reports/         Generated reports
docs/            Architecture and demonstration documentation