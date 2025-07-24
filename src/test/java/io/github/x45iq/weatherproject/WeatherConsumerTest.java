package io.github.x45iq.weatherproject;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class WeatherConsumerTest {
    private WeatherConsumer weatherConsumer;
    private final ObjectMapper testMapper = new ObjectMapper();

    @BeforeEach
    void setUp() throws Exception {
        try (MockedConstruction<KafkaConsumer> mock = Mockito.mockConstruction(KafkaConsumer.class)) {
            weatherConsumer = new WeatherConsumer();
        }

        Field mapperField = WeatherConsumer.class.getDeclaredField("objectMapper");
        mapperField.setAccessible(true);
        mapperField.set(weatherConsumer, testMapper);
    }

    @Test
    void testMessageProcessing() throws Exception {
        WeatherData testData = new WeatherData("Москва", 25, "солнечно", "2023-07-20");
        String json = testMapper.writeValueAsString(testData);

        ConsumerRecord<String, String> record = new ConsumerRecord<>("weather", 0, 0, "key", json);

        Method method = WeatherConsumer.class.getDeclaredMethod("processRecord", ConsumerRecord.class);
        method.setAccessible(true);
        method.invoke(weatherConsumer, record);

        Field dataField = WeatherConsumer.class.getDeclaredField("cityWeatherData");
        dataField.setAccessible(true);

        Map<String, List<WeatherData>> data = (Map<String, List<WeatherData>>) dataField.get(weatherConsumer);

        assertFalse(data.get("Москва").isEmpty());
    }

    @Test
    void testProcessRecord_ValidData() throws Exception {
        WeatherData testData = new WeatherData("Москва", 25, "солнечно", "2023-07-20");
        String json = testMapper.writeValueAsString(testData);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("weather", 0, 0, "key", json);

        weatherConsumer.processRecord(record);

        Field dataField = WeatherConsumer.class.getDeclaredField("cityWeatherData");
        dataField.setAccessible(true);
        Map<String, List<WeatherData>> data = (Map<String, List<WeatherData>>) dataField.get(weatherConsumer);

        assertFalse(data.isEmpty());
        assertEquals(1, data.get("Москва").size());
        assertEquals(testData, data.get("Москва").get(0));
    }
}