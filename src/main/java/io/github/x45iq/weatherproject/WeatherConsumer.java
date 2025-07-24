package io.github.x45iq.weatherproject;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class WeatherConsumer  implements WeatherService{
    private static final String TOPIC = "weather";
    private static final String BOOTSTRAP_SERVERS =
            System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
    private static final String GROUP_ID = "weather-analytics";

    private final KafkaConsumer<String, String> consumer;
    private final ObjectMapper objectMapper;

    private final Map<String, List<WeatherData>> cityWeatherData = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> cityRainyDays = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> citySunnyDays = new ConcurrentHashMap<>();
    private WeatherData hottestWeather = null;
    private String coldestCity = null;
    private double lowestAverageTemp = Double.MAX_VALUE;

    public WeatherConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        this.consumer = new KafkaConsumer<>(props);
        this.objectMapper = new ObjectMapper();

        consumer.subscribe(Collections.singletonList(TOPIC));
    }

    public void start() {
        System.out.println("Запуск консьюмера погодных данных...");

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record); // Используем новый метод
                }

                if (System.currentTimeMillis() % 10000 < 1000) {
                    printAnalytics();
                }
            }
        } catch (Exception e) {
            System.err.println("Ошибка в консьюмере: " + e.getMessage());
        } finally {
            consumer.close();
        }
    }
    void processRecord(ConsumerRecord<String, String> record) {
        try {
            WeatherData weather = objectMapper.readValue(record.value(), WeatherData.class);
            processWeatherData(weather);
            System.out.printf("Получено: %s%n", weather);
        } catch (JsonProcessingException e) {
            System.err.println("Ошибка парсинга JSON: " + e.getMessage());
        }
    }

    private void processWeatherData(WeatherData weather) {
        String city = weather.getCity();

        cityWeatherData.computeIfAbsent(city, k -> new ArrayList<>()).add(weather);

        if ("дождь".equals(weather.getCondition())) {
            cityRainyDays.computeIfAbsent(city, k -> new AtomicInteger(0)).incrementAndGet();
        }

        if ("солнечно".equals(weather.getCondition())) {
            citySunnyDays.computeIfAbsent(city, k -> new AtomicInteger(0)).incrementAndGet();
        }

        if (hottestWeather == null || weather.getTemperature() > hottestWeather.getTemperature()) {
            hottestWeather = weather;
        }

        updateColdestCity();
    }

    private double getAvgTempForDate(String city, String date) {
        List<WeatherData> cityData = cityWeatherData.get(city);
        if (cityData == null) {
            return 0.0;
        }

        return cityData.stream()
                .filter(data -> date.equals(data.getDate()))
                .mapToInt(WeatherData::getTemperature)
                .average()
                .orElse(0.0);
    }

    private void updateColdestCity() {
        coldestCity = null;
        lowestAverageTemp = Double.MAX_VALUE;

        for (String city : cityWeatherData.keySet()) {
            Set<String> uniqueDates = cityWeatherData.get(city).stream()
                    .map(WeatherData::getDate)
                    .collect(Collectors.toSet());

            double cityAvg = uniqueDates.stream()
                    .mapToDouble(date -> getAvgTempForDate(city, date))
                    .average()
                    .orElse(Double.MAX_VALUE);

            if (cityAvg < lowestAverageTemp) {
                lowestAverageTemp = cityAvg;
                coldestCity = city;
            }
        }
    }

    private void printAnalytics() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("                    АНАЛИТИКА ПОГОДЫ");
        System.out.println("=".repeat(60));

        String rainiest = cityRainyDays.entrySet().stream()
                .max(Map.Entry.comparingByValue(Comparator.comparingInt(AtomicInteger::get)))
                .map(entry -> entry.getKey() + " (" + entry.getValue().get() + " дней)")
                .orElse("Нет данных");
        System.out.println(" Самый дождливый город: " + rainiest);

        String sunniest = citySunnyDays.entrySet().stream()
                .max(Map.Entry.comparingByValue(Comparator.comparingInt(AtomicInteger::get)))
                .map(entry -> entry.getKey() + " (" + entry.getValue().get() + " дней)")
                .orElse("Нет данных");
        System.out.println(" Самый солнечный город: " + sunniest);

        if (hottestWeather != null) {
            System.out.printf(" Самая жаркая погода: %d°C в %s (%s)%n",
                    hottestWeather.getTemperature(),
                    hottestWeather.getCity(),
                    hottestWeather.getDate());
        }

        if (coldestCity != null) {
            System.out.printf(" Самая низкая средняя температура: %.1f°C в %s%n",
                    lowestAverageTemp, coldestCity);
        }

        System.out.println(" 2+ дождливых дня:");
        cityRainyDays.entrySet().stream()
                .filter(entry -> entry.getValue().get() >= 2)
                .forEach(entry -> System.out.printf("   - %s (%d дождливых дня)%n",
                        entry.getKey(), entry.getValue().get()));

        System.out.println("=".repeat(60) + "\n");
    }

    public void close() {
        if (consumer != null) {
            consumer.close();
        }
    }
}