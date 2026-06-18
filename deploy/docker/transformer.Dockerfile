FROM maven:3.9-eclipse-temurin-21 AS builder
WORKDIR /build
COPY services/transformer/pom.xml .
RUN mvn dependency:go-offline -q
COPY services/transformer/src ./src
RUN mvn package -DskipTests -q

FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S meridian && adduser -S meridian -G meridian
COPY --from=builder /build/target/meridian-transformer-1.0.0.jar /app/job.jar
USER meridian
ENTRYPOINT ["java", "-jar", "/app/job.jar"]
