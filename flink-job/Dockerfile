# Stage 1: Build fat JAR
FROM maven:3.9.6-eclipse-temurin-11 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -q
COPY src ./src
RUN mvn clean package -DskipTests -q

# Stage 2: Submit JAR to the Flink cluster
FROM flink:1.18.0-java11
WORKDIR /opt/flink/usrlib
COPY --from=build /app/target/flink-job-1.0.0.jar flink-job.jar

# The entrypoint submits the job to the running JobManager.
# docker-compose depends_on ensures jobmanager is healthy before this runs.
ENTRYPOINT ["flink", "run", \
            "-m", "jobmanager:8081", \
            "/opt/flink/usrlib/flink-job.jar"]
