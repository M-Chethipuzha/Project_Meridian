FROM openjdk:21-slim AS build
WORKDIR /src
COPY pom.xml .
RUN apt-get update && apt-get install -y maven && rm -rf /var/lib/apt/lists/*
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn package -B -DskipTests

FROM eclipse-temurin:21-jre-alpine
RUN adduser -D -u 1001 meridian
USER meridian
COPY --from=build /src/target/meridian-transformer-1.0.0.jar /transformer.jar
ENTRYPOINT ["java", "-jar", "/transformer.jar"]
