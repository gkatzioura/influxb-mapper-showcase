package com.gkatzioura.mapper.showcase;

import java.util.logging.Logger;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import static com.gkatzioura.mapper.showcase.ShowCaseConstants.DATABASE;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.asc;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.desc;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.eq;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.select;

public class OrderByShowcase {

    private static final Logger LOGGER = Logger.getLogger(IntoShowcase.class.getName());

    public static void main(String[] args) {
        InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086", "root", "root");
        orderAscending(influxDB);
        orderDescending(influxDB);
    }


    private static void orderAscending(InfluxDB influxDB) {
        Query query = select().from(DATABASE, "h2o_feet")
                               .where(eq("location","santa_monica"))
                               .orderBy(asc());
        LOGGER.info("Executing query "+query.getCommand());
        QueryResult queryResult = influxDB.query(query);
    }

    private static void orderDescending(InfluxDB influxDB) {
        Query query = select().from(DATABASE, "h2o_feet")
                               .where(eq("location","santa_monica"))
                               .orderBy(desc());
        LOGGER.info("Executing query "+query.getCommand());
        QueryResult queryResult = influxDB.query(query);
    }

}
