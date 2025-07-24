package io.github.x45iq.weatherproject;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class WeatherProducerTest {
    private WeatherProducer producer;
    private KafkaProducer<String, String> mockProducer;

    @BeforeEach
    void setUp() throws Exception {
        mockProducer = mock(KafkaProducer.class);
        producer = new WeatherProducer();

        Field producerField = WeatherProducer.class.getDeclaredField("producer");
        producerField.setAccessible(true);
        producerField.set(producer, mockProducer);
    }

    @Test
    void testStartSendingSchedule() {
        producer.start();

        verify(mockProducer, timeout(3000)).send(any(), any());
    }

    @Test
    void testWeatherGeneration() throws Exception {
        Method method = WeatherProducer.class.getDeclaredMethod("generateRandomWeather", String.class, String.class);
        method.setAccessible(true);

        WeatherData weather = (WeatherData) method.invoke(producer, "Москва", "2023-07-20");

        assertNotNull(weather);
        assertEquals("Москва", weather.getCity());
        assertTrue(weather.getTemperature() >= 0 && weather.getTemperature() <= 35);
    }
}