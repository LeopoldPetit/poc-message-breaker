FROM maven:3.9.9-eclipse-temurin-21 AS build
ARG MODULE
WORKDIR /workspace
COPY pom.xml ./
COPY common-model/pom.xml common-model/pom.xml
COPY order-producer/pom.xml order-producer/pom.xml
COPY order-consumer-kafka/pom.xml order-consumer-kafka/pom.xml
COPY order-consumer-rabbitmq/pom.xml order-consumer-rabbitmq/pom.xml
COPY common-model/src common-model/src
COPY ${MODULE}/src ${MODULE}/src
RUN mvn -B -pl ${MODULE} -am package -DskipTests

FROM eclipse-temurin:21-jre
ARG MODULE
COPY --from=build /workspace/${MODULE}/target/*.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
