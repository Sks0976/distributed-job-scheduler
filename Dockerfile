# ---- build ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src ./src
RUN mvn -B -q package -DskipTests

# ---- run ----
FROM eclipse-temurin:21-jre
WORKDIR /app
RUN useradd --system --uid 1001 scheduler
COPY --from=build /app/target/distributed-job-scheduler-*.jar app.jar
USER scheduler
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
