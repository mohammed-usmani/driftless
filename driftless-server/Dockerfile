# Build from the repository root: docker build -f driftless-server/Dockerfile .
FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY .mvn .mvn
COPY mvnw pom.xml ./
COPY driftless-server driftless-server
COPY driftless-loadgen/pom.xml driftless-loadgen/pom.xml
RUN --mount=type=cache,target=/root/.m2 \
    ./mvnw -B -q -pl driftless-server -am package -Dmaven.test.skip=true

FROM eclipse-temurin:21-jre
RUN useradd --system --uid 10001 --no-create-home driftless
WORKDIR /app
COPY --from=build /src/driftless-server/target/driftless-server-0.1.0-SNAPSHOT.jar app.jar
USER 10001
EXPOSE 8180 8189
HEALTHCHECK --interval=10s --timeout=3s --start-period=90s --retries=5 \
    CMD curl -fsS http://localhost:8189/actuator/health/readiness > /dev/null || exit 1
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
