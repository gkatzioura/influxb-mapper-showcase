package com.gkatzioura.mapper.showcase;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;

@Measurement(name = "h2o_feet", timeUnit = TimeUnit.SECONDS)
public class LocationWithDescription {

    @Column(name = "time")
    private Instant time;

    @Column(name = "level description")
    private String levelDescription;

    @Column(name = "location")
    private String location;

    public Instant getTime() {
        return time;
    }

    public void setTime(Instant time) {
        this.time = time;
    }

    public String getLevelDescription() {
        return levelDescription;
    }

    public void setLevelDescription(String levelDescription) {
        this.levelDescription = levelDescription;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }
}
