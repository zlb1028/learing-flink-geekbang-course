package com.geekbang.flink.windows;

import com.geekbang.flink.sources.GeneratorTableSource;
import com.geekbang.flink.sql.SQLDemo;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;


public class TumbleWindow {
    public static void main(String args[]) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);

        String outputPath = params.get("outputPath", "data/table/windows/OverWindow");

        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3,
                Time.of(10, TimeUnit.SECONDS)
        ));

        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        sEnv.enableCheckpointing(4000);
        sEnv.getConfig().setAutoWatermarkInterval(1000);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv);

        tEnv.registerTableSource("table1", new GeneratorTableSource(10, 100, 60, 0));
        tEnv.registerTableSource("table2", new GeneratorTableSource(5, 0.2f, 60, 5));

        int overWindowSizeSeconds = 10;

        String overQuery = String.format(
                "SELECT " +
                        "  key, " +
                        "  rowtime, " +
                        "  COUNT(*) OVER (PARTITION BY key ORDER BY rowtime RANGE BETWEEN INTERVAL '%d' SECOND PRECEDING AND CURRENT ROW) AS cnt " +
                        "FROM table1",
                overWindowSizeSeconds);
        Table result = tEnv.sqlQuery(overQuery);

        // convert Table into append-only DataStream
        DataStream<Row> resultStream =
                tEnv.toAppendStream(result, Types.ROW(Types.INT, Types.SQL_TIMESTAMP, Types.LONG));

        final StreamingFileSink<Row> sink = StreamingFileSink
                .forRowFormat(new Path(outputPath), (Encoder<Row>) (element, stream) -> {
                    PrintStream out = new PrintStream(stream);
                    out.println(element.toString());
                })
                .withBucketAssigner(new SQLDemo.KeyBucketAssigner())
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        resultStream.print();

        resultStream
                // inject a KillMapper that forwards all records but terminates the first execution attempt
                .map(new SQLDemo.KillMapper()).setParallelism(1)
                // add sink function
                .addSink(sink).setParallelism(1);

        sEnv.execute();
    }
}
