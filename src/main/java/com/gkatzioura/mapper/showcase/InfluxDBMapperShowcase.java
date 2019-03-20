package com.gkatzioura.mapper.showcase;

import java.time.Instant;
import java.util.List;
import java.util.logging.Logger;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.impl.InfluxDBImpl;
import org.influxdb.impl.InfluxDBMapper;

public class InfluxDBMapperShowcase {

    private static final Logger LOGGER = Logger.getLogger(InfluxDBMapperShowcase.class.getName());

    public static void main(String[] args) {
        InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086", "root", "root");

        InfluxDBMapper influxDBMapper = new InfluxDBMapper(influxDB);
        List<H2OFeetMeasurement> h2OFeetMeasurements = influxDBMapper.query(H2OFeetMeasurement.class);

        LOGGER.info("test the measurement");

        H2OFeetMeasurement h2OFeetMeasurement = new H2OFeetMeasurement();
        h2OFeetMeasurement.setTime(Instant.now());
        h2OFeetMeasurement.setLevelDescription("Just a test");
        h2OFeetMeasurement.setLocation("London");
        h2OFeetMeasurement.setWaterLevel(1.4d);

        influxDBMapper.save(h2OFeetMeasurement);

        List<H2OFeetMeasurement> measurements = influxDBMapper.query(H2OFeetMeasurement.class);

        H2OFeetMeasurement h2OFeetMeasurement1 = measurements.get(measurements.size()-1);
        assert h2OFeetMeasurement1.getLevelDescription().equals("Just a test");
    }

}
