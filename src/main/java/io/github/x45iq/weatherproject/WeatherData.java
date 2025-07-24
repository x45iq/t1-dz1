package io.github.x45iq.weatherproject;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

public class WeatherData {
    @JsonProperty("city")
    private String city;

    @JsonProperty("temperature")
    private int temperature;

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        WeatherData that = (WeatherData) o;
        return temperature == that.temperature && Objects.equals(city, that.city) && Objects.equals(condition, that.condition) && Objects.equals(date, that.date) && Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(city, temperature, condition, date, timestamp);
    }

    @JsonProperty("condition")
    private String condition;

    @JsonProperty("date")
    private String date;

    @JsonProperty("timestamp")
    private String timestamp;

    public WeatherData() {}

    public WeatherData(String city, int temperature, String condition, String date) {
        this.city = city;
        this.temperature = temperature;
        this.condition = condition;
        this.date = date;
        this.timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }

    public String getCity() { return city; }
    public void setCity(String city) { this.city = city; }

    public int getTemperature() { return temperature; }
    public void setTemperature(int temperature) { this.temperature = temperature; }

    public String getCondition() { return condition; }
    public void setCondition(String condition) { this.condition = condition; }

    public String getDate() { return date; }
    public void setDate(String date) { this.date = date; }

    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return String.format("WeatherData{city='%s', temperature=%d°C, condition='%s', date='%s'}",
                city, temperature, condition, date);
    }
}