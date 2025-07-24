package io.github.x45iq.weatherproject;

public class WeatherKafkaApp {
    public static void main(String[] args) {
        String mode = args.length > 0 ? args[0] : System.getenv("RUN_MODE");

        if (mode == null || mode.isEmpty()) {
            System.out.println("Использование: java WeatherKafkaApp [producer|consumer]");
            return;
        }

        WeatherService service;
        switch(mode) {
            case "producer":
                service = new WeatherProducer();
                break;
            case "consumer":
                service = new WeatherConsumer();
                break;
            default:
                throw new IllegalArgumentException("Неизвестный режим");
        }

        Runtime.getRuntime().addShutdownHook(new Thread(service::close));
        service.start();
    }
}
