package io.github.x45iq.weatherproject;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class WeatherDataTest {

    @Test
    void testConstructorAndGetters() {
        WeatherData data = new WeatherData("Москва", 25, "солнечно", "2023-07-20");

        assertEquals("Москва", data.getCity());
        assertEquals(25, data.getTemperature());
        assertEquals("солнечно", data.getCondition());
        assertEquals("2023-07-20", data.getDate());
        assertNotNull(data.getTimestamp());
    }

    @Test
    void testSetters() {
        WeatherData data = new WeatherData();
        data.setCity("Санкт-Петербург");
        data.setTemperature(18);
        data.setCondition("дождь");
        data.setDate("2023-07-21");
        data.setTimestamp("2023-07-21T12:00:00");

        assertEquals("Санкт-Петербург", data.getCity());
        assertEquals(18, data.getTemperature());
        assertEquals("дождь", data.getCondition());
        assertEquals("2023-07-21", data.getDate());
        assertEquals("2023-07-21T12:00:00", data.getTimestamp());
    }

    @Test
    void testToString() {
        WeatherData data = new WeatherData("Казань", 30, "облачно", "2023-07-22");
        String expected = "WeatherData{city='Казань', temperature=30°C, condition='облачно', date='2023-07-22'}";
        assertEquals(expected, data.toString());
    }
}