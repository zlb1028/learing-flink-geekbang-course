package com.geekbang.flink.table;

import com.geekbang.flink.sources.GeneratorTableSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;


public class TableToDataStream {
    public static void main(String args[]) {
        ParameterTool params = ParameterTool.fromArgs(args);

        String outputPath = params.get("outputPath", "data/tmp/output");

        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3,
                Time.of(10, TimeUnit.SECONDS)
        ));
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        sEnv.enableCheckpointing(4000);
        sEnv.getConfig().setAutoWatermarkInterval(1000);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv);

        tEnv.registerTableSource("table", new GeneratorTableSource(10, 100, 60, 0));

        Table table = tEnv.scan("table");

        DataStream<Row> appendStream = tEnv.toAppendStream(table, Row.class);

        appendStream.print();


    }
}
