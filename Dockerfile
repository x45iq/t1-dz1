FROM maven:3.8.6-openjdk-11 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

FROM openjdk:11-jre-slim
WORKDIR /app
COPY --from=build /app/target/weatherProject-0.0.1-jar-with-dependencies.jar /app/app.jar

ENV MAIN_CLASS=io.github.x45iq.weatherproject.WeatherKafkaApp

CMD ["sh", "-c", "java -cp app.jar $MAIN_CLASS $RUN_MODE"]
