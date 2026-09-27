# Architecture Design

## 1. Project Overview

The Ride-Hailing Data Engineering Platform is an end-to-end data platform designed to provide near-real-time visibility into fleet activity while reconciling vehicle performance against daily fuel and maintenance expenses.

The system simulates two data sources:

1. A continuous streaming source producing vehicle telemetry events.
2. A daily expense source producing fuel and maintenance records.

The platform processes these sources and produces fleet utilization, earnings, and profitability information.

## 2. Architecture Choice

### Selected Architecture: Kappa Architecture

A Kappa-oriented architecture was selected because the primary business data is event-oriented and the system requires near-real-time processing.

The streaming telemetry and daily expense records are both published to Apache Kafka. Apache Spark Structured Streaming consumes the Kafka events and performs cleaning, enrichment, aggregation, and profitability calculations.

The daily expense source originates as a file, but the records are converted into Kafka events so that the downstream processing logic remains unified.

### Reasons for Selecting Kappa

- Single primary processing path
- Reduced duplication of transformation logic
- Near-real-time processing
- Kafka provides a durable event stream
- Historical events can be replayed for recomputation
- Suitable for the relatively small scope of the simulated platform

### Trade-offs

Kappa requires Kafka to act as an important durable event backbone. Retention and replay therefore become important operational considerations.

A large-scale production system may require additional storage and governance mechanisms for long-term historical data.

## 3. Rejected Alternative: Lambda Architecture

Lambda architecture would maintain separate speed and batch processing paths.

Although Lambda can be useful when independent batch processing of large historical datasets is required, it introduces duplicated processing logic and creates an additional consistency concern between the batch and speed layers.

For this project, a unified streaming-oriented processing path is simpler to implement and demonstrate.

## 4. Simulated Time

The project compresses simulated time for demonstration.

One simulated business day corresponds to approximately five real-time minutes.

Telemetry events are generated approximately every three seconds.

The compressed clock allows daily reconciliation and real-time monitoring to be demonstrated within a short assessment session.

## 5. Components

### Python Telemetry Producer

Continuously generates vehicle telemetry events containing:

- trip_id
- driver_id
- vehicle_id
- latitude
- longitude
- speed
- status
- fare
- timestamp

### Python Expense Producer

Generates daily vehicle expense records containing:

- vehicle_id
- fuel_cost
- maintenance_cost
- distance_covered
- service_flag
- expense_date

### Apache Kafka

Kafka provides the event ingestion layer.

Topics:

- `ride_telemetry`
- `vehicle_expenses`

### Apache Spark Structured Streaming

Spark performs:

- schema validation
- data cleaning
- enrichment
- time-window aggregation
- vehicle-level aggregation
- earnings calculation
- profitability calculation
- stream/batch reconciliation

### Apache Airflow

Airflow orchestrates the daily expense workflow.

The workflow generates, validates, publishes, and reconciles the daily expense data.

### PostgreSQL

PostgreSQL stores processed results for API and dashboard access.

### FastAPI

FastAPI exposes queryable endpoints for:

- fleet utilization
- zone metrics
- vehicle metrics
- profitability
- alerts
- health status

### Prometheus

Prometheus collects application and pipeline metrics.

### Grafana

Grafana provides operational monitoring dashboards.

## 6. Observability

The platform implements:

- structured application logs
- pipeline health checks
- Prometheus metrics
- Grafana dashboards
- telemetry freshness monitoring
- excessive-idle alerts
- processing error monitoring

## 7. Business Outputs

The platform provides:

- active vehicle count
- idle vehicle count
- idle ratio
- trips per hour
- earnings by zone
- vehicle-level earnings
- fuel costs
- maintenance costs
- vehicle profitability
- profitability classification
- operational alerts
- daily reconciliation reports