package com.course.kafka.broker.message;

public class LocationMessage {

    private String weather;
    private String traffic;

    public String getWeather() {
        return weather;
    }

    public void setWeather(String weather) {
        this.weather = weather;
    }

    public String getTraffic() {
        return traffic;
    }

    public void setTraffic(String traffic) {
        this.traffic = traffic;
    }

    @Override
    public String toString() {
        return "LocationMessage [weather=" + weather + ", traffic=" + traffic + "]";
    }

}
