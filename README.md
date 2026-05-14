# Real-Time ML Feature Engineering Pipeline

This project is a real-time feature engineering pipeline built using Apache Kafka, Apache Flink, and Spring Boot. It processes continuous mock user engagement data to calculate streaming features for machine learning models.

## Architecture

1. **Spring Boot Mock Data Producer**: Simulates real-time clickstream data, including purposefully late events (35-90 seconds delayed) to test robust stream handling. Dispatches JSON messages to the `user-events` Kafka topic.
2. **Apache Kafka (Confluent)**: Acts as the central event bus with topics: `user-events`, `content-metadata`, and `feature-store`.
3. **Apache Flink Job**: Consumes `user-events`, uses a `WatermarkStrategy` handling 30-second bounded out-of-orderness, aggregates data using sliding/tumbling windows, and sinks standard feature representations to the `feature-store` Kafka topic.
4. **Spring Boot Consumer Dashboard**: Consumes the computed features from `feature-store` and pushes them to a web frontend via STOMP WebSockets for instant, real-time visualization of latency and feature freshness.

## Prerequisites

* [Docker](https://docs.docker.com/get-docker/) & [Docker Compose](https://docs.docker.com/compose/install/)
* Java 17 (Required only if testing/running outside of Docker)
* Maven 3.8+ (Required only if testing/running outside of Docker)

## Environment Configuration

A `.env.example` is provided in the repository. Do not commit actual `.env` files containing secrets. If applying customized ports or environments, copy the file:
```bash
cp .env.example .env
```

## Running the Pipeline

The entire system is containerized. To build and start the orchestration:

```bash
# Start the pipeline in detached mode
docker-compose up --build -d
```

### Services & Endpoints

Once the services are healthy, you can access the following UIs:

* **Real-Time ML Dashboard**: http://localhost:8080
* **Flink Web UI**: http://localhost:8081

### Tearing Down

To stop the pipeline and remove the containers, networks, and volumes (cleaning up the state):

```bash
docker-compose down -v
```

## Project Structure

* `src/main/java.../producer/`: Contains the Spring Boot mock data generation mechanism and the WebSocket consumer forwarding properties.
* `src/main/java.../flink/`: Contains `FlinkFeatureJob.java` which houses the stream processing architecture, watermarks, and windowing functions.
* `src/main/resources/static/`: Houses the dashboard UI (`index.html`) using raw HTML/JS and SockJS/Stomp.
* `docker-compose.yml`: Orchestration file bundling Kafka, Zookeeper, Flink, Kafka-Init, and the Spring Boot App.
* `ANALYSIS.md`: Explains the divergence between streaming and batch context with out-of-order event handling.
* `submission.json`: Details the applicant and environment targets for the automated reviewer.
