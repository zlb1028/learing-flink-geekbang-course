package com.geekbang.flink.sql;

import com.geekbang.flink.sources.GeneratorTableSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

/**
 * Created by Onizuka on 2019/8/24
 */
public class SQLDemo2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        sEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3,
                Time.of(10, TimeUnit.SECONDS)
        ));

        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        sEnv.enableCheckpointing(4000);

        sEnv.getConfig().setAutoWatermarkInterval(1000);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv);

        tEnv.registerTableSource("input_table", new GeneratorTableSource(10, 10, 10, 10));

        int overWindowSizeSeconds = 10;

        Table table = tEnv.scan("input_table");

        String querySql = String.format(
                "SELECT " +
                        "  key, " +
                        "  rowtime, " +
                        "  COUNT(*) OVER (PARTITION BY key ORDER BY rowtime RANGE BETWEEN INTERVAL '%d' SECOND PRECEDING AND CURRENT ROW) AS cnt " +
                        "FROM input_table",
                overWindowSizeSeconds);

        table.printSchema();

//        // get Table for SQL query
        Table result = tEnv.sqlQuery(querySql);
//
//        // convert Table into append-only DataStream
        DataStream<Row> resultStream = tEnv.toAppendStream(result, Types.ROW(Types.INT, Types.SQL_TIMESTAMP, Types.LONG));

        resultStream.print();

        sEnv.execute();
    }
}
