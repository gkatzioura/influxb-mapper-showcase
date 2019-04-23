package com.gkatzioura.mapper.showcase;

import java.util.logging.Logger;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.querybuilder.RawText;

import static com.gkatzioura.mapper.showcase.ShowCaseConstants.DATABASE;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.eq;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.gte;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.lte;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.select;
import static org.influxdb.querybuilder.FunctionFactory.time;
import static org.influxdb.querybuilder.time.DurationLiteral.MINUTE;

public class IntoShowcase {

    private static final Logger LOGGER = Logger.getLogger(IntoShowcase.class.getName());

    public static void main(String[] args) {
        InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086", "root", "root");
        //renameDatabase(influxDB);
        //writeResultsToMeasurement(influxDB);
        writeAggregatedToMeasurement(influxDB);
    }

    private static void writeResultsToMeasurement(InfluxDB influxDB) {
        Query query = select().column("water_level")
                               .into("h2o_feet_copy_1")
                               .from(DATABASE,"h2o_feet")
                               .where(eq("location","coyote_creek"));
        LOGGER.info("Executing query "+query.getCommand());
        QueryResult queryResult = influxDB.query(query);
    }

    private static void writeAggregatedToMeasurement(InfluxDB influxDB) {
        Query query = select()
                .mean("water_level")
                .into("all_my_averages")
                .from(DATABASE,"h2o_feet")
                .where(eq("location","coyote_creek"))
                .and(gte("time","2015-08-18T00:00:00Z"))
                .and(lte("time","2015-08-18T00:30:00Z"))
                .groupBy(time(12l,MINUTE));
        LOGGER.info("Executing query "+query.getCommand());
        QueryResult queryResult = influxDB.query(query);
        System.out.println("");
    }

    private static void renameDatabase(InfluxDB influxDB) {
        Query query = select()
                .into("\"copy_NOAA_water_database\".\"autogen\".:MEASUREMENT")
                .from(DATABASE, "\"NOAA_water_database\".\"autogen\"./.*/")
                .groupBy(new RawText("*"));
        LOGGER.info("Executing query "+query.getCommand());
        QueryResult queryResult = influxDB.query(query);
        System.out.println("Copy");
    }



    //LOGGER.info("Executing query "+query.getCommand());
}
