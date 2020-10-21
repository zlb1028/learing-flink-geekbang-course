package com.geekbang.flink.sql;

import com.geekbang.flink.sources.ProcessTimeTableSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

/**
 * Created by Onizuka on 2019/9/11
 */
public class ProcessTimeStreamSQL {
    public static void main(String args[]) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        String outputPath = params.getRequired("outputPath");

        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        sEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3,
                Time.of(10, TimeUnit.SECONDS)
        ));
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        sEnv.enableCheckpointing(4000);

        sEnv.getConfig().setAutoWatermarkInterval(1000);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv);

        tEnv.registerTableSource("table1", new ProcessTimeTableSource("table1", 5, 0.2f, 60, 0));

        tEnv.registerTableSource("table2", new ProcessTimeTableSource("table2", 1, 0.2f, 60, 5));

        int tumbleWindowSizeSeconds = 10;

        String tumbleQuery = String.format(
                "SELECT " +
                        "  key, " +
                        "  COUNT(*) AS count_num, " +
                        "  TUMBLE_START(proctime, INTERVAL '%d' SECOND) AS wStart, " +
                        "  TUMBLE_PROCTIME(proctime, INTERVAL '%d' SECOND) AS proctime, " +
                        "  TUMBLE_END(proctime, INTERVAL '%d' SECOND) AS wEnd " +
                        "FROM table1 " +
                        "WHERE proctime > TIMESTAMP '1970-01-01 00:00:00' " +
                        "GROUP BY key, TUMBLE(proctime, INTERVAL '%d' SECOND)",
                tumbleWindowSizeSeconds,
                tumbleWindowSizeSeconds,
                tumbleWindowSizeSeconds,
                tumbleWindowSizeSeconds);

        Table tumbleWindowTable = tEnv.sqlQuery(tumbleQuery);

        DataStream<Row> tumbleWindowStream = tEnv.toAppendStream(tumbleWindowTable, Types.ROW(Types.INT, Types.LONG, Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP));

        tumbleWindowStream.print();

        sEnv.execute();

    }
}
