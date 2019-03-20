package com.gkatzioura.mapper.showcase;

import java.util.List;
import java.util.logging.Logger;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.impl.InfluxDBMapper;

import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.cop;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.eq;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.gt;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.lt;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.neq;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.op;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.select;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.subTime;
import static org.influxdb.querybuilder.Operations.MUL;
import static org.influxdb.querybuilder.time.DurationLiteral.DAY;

public class QueryBuilderSelectShowcase {

    private static final String DATABASE = "NOAA_water_database";

    private static final Logger LOGGER = Logger.getLogger(QueryBuilderSelectShowcase.class.getName());

    public static void main(String[] args) {
        InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086", "root", "root");

        InfluxDBMapper influxDBMapper = new InfluxDBMapper(influxDB);

        selectAll(influxDBMapper);
        selectGreater(influxDBMapper);
        selectWithOperation(influxDB);
        selectByStringFieldKeyValue(influxDBMapper);
        selectByTagAndFieldValues(influxDBMapper);
        selectByTimestamps(influxDBMapper);
        selectFields(influxDBMapper);
    }

    private static final void selectAll(InfluxDBMapper influxDBMapper) {
        Query query = select().from(DATABASE,"h2o_feet");
        List<H2OFeetMeasurement> h2OFeetMeasurements = influxDBMapper.query(query, H2OFeetMeasurement.class);
    }

    private static final void selectGreater(InfluxDBMapper influxDBMapper) {
        Query query = select().from(DATABASE,"h2o_feet").where(gt("water_level",8));
        LOGGER.info("Executing query "+query.getCommand());
        List<H2OFeetMeasurement> higherThanMeasurements = influxDBMapper.query(query, H2OFeetMeasurement.class);
    }

    private static final void selectWithOperation(InfluxDB influxDB) {
        Query query = select().op(op(cop("water_level",MUL,2),"+",4)).from(DATABASE,"h2o_feet");
        LOGGER.info("Executing query "+query.getCommand());
        QueryResult queryResult = influxDB.query(query);
    }

    private static final void selectByStringFieldKeyValue(InfluxDBMapper influxDBMapper) {
        Query query = select().from(DATABASE,"h2o_feet").where(eq("location","santa_monica"));
        LOGGER.info("Executing query "+query.getCommand());
        List<H2OFeetMeasurement> h2OFeetMeasurements = influxDBMapper.query(query, H2OFeetMeasurement.class);
    }

    private static final void selectByTagAndFieldValues(InfluxDBMapper influxDBMapper) {
        Query query = select().column("water_level").from(DATABASE,"h2o_feet")
                              .where(neq("location","santa_monica"))
                              .andNested()
                              .and(lt("water_level",-0.59))
                              .or(gt("water_level",9.95))
                              .close();
        LOGGER.info("Executing query "+query.getCommand());
        List<H2OFeetMeasurement> h2OFeetMeasurements = influxDBMapper.query(query, H2OFeetMeasurement.class);
    }

    private static final void selectByTimestamps(InfluxDBMapper influxDBMapper) {
        Query query = select().from(DATABASE,"h2o_feet")
                              .where(gt("time",subTime(7,DAY)));
        LOGGER.info("Executing query "+query.getCommand());
        List<H2OFeetMeasurement> h2OFeetMeasurements = influxDBMapper.query(query, H2OFeetMeasurement.class);
    }

    private static final void selectFields(InfluxDBMapper influxDBMapper) {
        Query selectFields = select("level description","location").from(DATABASE,"h2o_feet");
        List<LocationWithDescription> locationWithDescriptions = influxDBMapper.query(selectFields, LocationWithDescription.class);
    }
}
