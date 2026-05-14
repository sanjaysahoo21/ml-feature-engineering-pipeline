# Real-Time ML Feature Engineering Pipeline

A production-style, real-time feature engineering pipeline built with **Apache Kafka**, **Apache Flink**, and **Spring Boot**, orchestrated with Docker Compose.

## Architecture

Three independent microservices communicate through Kafka:

```
producer/ ──► Kafka ──► flink-job/ ──► feature-store topic ──► dashboard/
              (user-events, content-metadata)                    (WebSocket UI)
```

| Service | Responsibility |
|---|---|
| `producer` | Simulates user interactions; seeds content metadata on startup; injects 5% late events (35–90 s) to test watermarking |
| `flink-job` | Consumes `user-events`, computes `click_rate`, `avg_dwell_time`, `engagement_rate`, `category_affinity_*`; sinks to `feature-store` |
| `dashboard` | Consumes `feature-store` and `flink-metrics`; serves a real-time WebSocket UI and REST API |

### Kafka Topics

| Topic | Partitions | Config |
|---|---|---|
| `user-events` | 3 | default |
| `content-metadata` | 1 | `cleanup.policy=compact` |
| `feature-store` | 1 | `cleanup.policy=compact` |
| `flink-metrics` | 1 | default |

### Flink Features Computed

| Feature | Window | Key |
|---|---|---|
| `click_rate` | Tumbling 1 h | `user_id` |
| `avg_dwell_time` | Tumbling 1 h | `user_id` |
| `engagement_rate` | Sliding 15 min / 5 min | `content_id` |
| `category_affinity_<cat>` | Tumbling 1 h (stream-table join) | `user_id` |

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/)
- Java 17 + Maven 3.8+ (only needed to build/run outside Docker)

## Quick Start

```bash
# 1. Copy environment config
cp .env.example .env

# 2. Build and start all services
docker-compose up --build -d

# 3. Watch service health
docker-compose ps
```

All services will be healthy within ~3–5 minutes on first run (Maven dependency download).

## Endpoints

| URL | Description |
|---|---|
| http://localhost:8082 | Observability Dashboard UI |
| http://localhost:8081 | Flink Web UI (job graph, watermarks, metrics) |
| http://localhost:8080/actuator/health | Producer health |
| http://localhost:8082/api/features/{entityId} | Latest features for any entity |
| http://localhost:8082/api/metrics | Pipeline health (late events, watermark lag) |

## Environment Variables

See `.env.example` for all variables. Key ones:

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Internal Kafka address |
| `KAFKA_EXTERNAL_PORT` | `9092` | Kafka port on the host |
| `FLINK_UI_PORT` | `8081` | Flink Web UI port |
| `PRODUCER_PORT` | `8080` | Producer actuator port |
| `DASHBOARD_PORT` | `8082` | Dashboard UI port |

## Project Structure

```
ml-feature-engineering-pipeline/
├── producer/               # Data-generator microservice (Spring Boot)
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/main/java/com/pipeline/producer/
│       ├── model/          # UserEvent, ContentMetadata
│       └── service/        # DataGeneratorService
├── flink-job/              # Stream-processing job (Apache Flink)
│   ├── Dockerfile          # Builds fat JAR; submits via flink run
│   ├── pom.xml             # Maven Shade plugin
│   └── src/main/java/com/pipeline/flink/
│       └── FlinkFeatureJob.java
├── dashboard/              # Observability dashboard microservice (Spring Boot)
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/main/java/com/pipeline/dashboard/
│       ├── config/         # WebSocketConfig
│       ├── controller/     # DashboardController (REST API)
│       └── service/        # FeatureConsumerService, FeatureStoreService, MetricsService
│   └── src/main/resources/static/index.html
├── docker-compose.yml
├── .env.example
├── ANALYSIS.md
├── batch_analysis.py       # Batch feature computation for divergence analysis
└── submission.json
```

## Stopping the Pipeline

```bash
docker-compose down -v
```

## Batch Analysis

To compare streaming vs. batch feature values (requires the pipeline to have been running):

```bash
pip install kafka-python pandas tabulate
python batch_analysis.py --bootstrap-servers localhost:9092
```

See `ANALYSIS.md` for the full comparison and late-event handling discussion.
