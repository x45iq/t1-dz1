package io.github.x45iq.weatherproject;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class WeatherProducer  implements WeatherService{
    private static final String TOPIC = "weather";
    private static final String BOOTSTRAP_SERVERS =
            System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;
    private final Random random;

    private final String[] cities = {
            "Москва", "Санкт-Петербург", "Магадан", "Чукотка", "Тюмень",
            "Новосибирск", "Екатеринбург", "Казань", "Сочи", "Владивосток"
    };

    private final String[] conditions = {"солнечно", "облачно", "дождь"};

    public WeatherProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        this.producer = new KafkaProducer<>(props);
        this.objectMapper = new ObjectMapper();
        this.random = new Random();
    }

    public void sendWeatherData() {
        if (producer == null) return;
        try {
            LocalDateTime startDate = LocalDateTime.now().minusDays(7);

            for (int day = 0; day < 7; day++) {
                String dateStr = startDate.plusDays(day).format(DateTimeFormatter.ISO_LOCAL_DATE);

                for (String city : cities) {
                    if (random.nextDouble() < 0.7) {
                        WeatherData weather = generateRandomWeather(city, dateStr);
                        String weatherJson = objectMapper.writeValueAsString(weather);

                        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, city, weatherJson);

                        producer.send(record, (metadata, exception) -> {
                            if (exception == null) {
                                System.out.printf("Отправлено: %s -> %s%n", city, weather);
                            } else {
                                System.err.printf("Ошибка отправки для %s: %s%n", city, exception.getMessage());
                            }
                        });

                        Thread.sleep(100);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Ошибка в продюсере: " + e.getMessage());
        }
    }

    private WeatherData generateRandomWeather(String city, String date) {
        int temperature = random.nextInt(36);
        String condition = conditions[random.nextInt(conditions.length)];
        return new WeatherData(city, temperature, condition, date);
    }

    public void start() {
        System.out.println("Запуск продюсера погодных данных...");

        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                sendWeatherData();
            }
        }, 0, 2000);
    }

    public void close() {
        if (producer != null) producer.close();
    }
}