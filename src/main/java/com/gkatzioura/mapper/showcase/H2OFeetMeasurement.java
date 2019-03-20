package com.gkatzioura.mapper.showcase;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;

@Measurement(name = "h2o_feet", database = "NOAA_water_database", timeUnit = TimeUnit.SECONDS)
public class H2OFeetMeasurement {

    @Column(name = "time")
    private Instant time;

    @Column(name = "level description")
    private String levelDescription;

    @Column(name = "location")
    private String location;

    @Column(name = "water_level")
    private Double waterLevel;

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

    public Double getWaterLevel() {
        return waterLevel;
    }

    public void setWaterLevel(Double waterLevel) {
        this.waterLevel = waterLevel;
    }
}
