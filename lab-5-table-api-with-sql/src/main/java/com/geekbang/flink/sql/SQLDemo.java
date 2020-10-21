package com.geekbang.flink.sql;

import com.geekbang.flink.sources.GeneratorTableSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class SQLDemo {

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        String outputPath = params.get("outputPath","data/tmp/output");

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
        int tumbleWindowSizeSeconds = 10;

        String overQuery = String.format(
                "SELECT " +
                        "  key, " +
                        "  rowtime, " +
                        "  COUNT(*) OVER (PARTITION BY key ORDER BY rowtime RANGE BETWEEN INTERVAL '%d' SECOND PRECEDING AND CURRENT ROW) AS cnt " +
                        "FROM table1",
                overWindowSizeSeconds);

        String tumbleQuery = String.format(
                "SELECT " +
                        "  key, " +
                        "  CASE SUM(cnt) / COUNT(*) WHEN 101 THEN 1 ELSE 99 END AS correct, " +
                        "  TUMBLE_START(rowtime, INTERVAL '%d' SECOND) AS wStart, " +
                        "  TUMBLE_ROWTIME(rowtime, INTERVAL '%d' SECOND) AS rowtime " +
                        "FROM (%s) " +
                        "WHERE rowtime > TIMESTAMP '1970-01-01 00:00:01' " +
                        "GROUP BY key, TUMBLE(rowtime, INTERVAL '%d' SECOND)",
                tumbleWindowSizeSeconds,
                tumbleWindowSizeSeconds,
                overQuery,
                tumbleWindowSizeSeconds);

        String joinQuery = String.format(
                "SELECT " +
                        "  t1.key, " +
                        "  t2.rowtime AS rowtime, " +
                        "  t2.correct," +
                        "  t2.wStart " +
                        "FROM table2 t1, (%s) t2 " +
                        "WHERE " +
                        "  t1.key = t2.key AND " +
                        "  t1.rowtime BETWEEN t2.rowtime AND t2.rowtime + INTERVAL '%d' SECOND",
                tumbleQuery,
                tumbleWindowSizeSeconds);

        String finalAgg = String.format(
                "SELECT " +
                        "  SUM(correct) AS correct, " +
                        "  TUMBLE_START(rowtime, INTERVAL '20' SECOND) AS rowtime " +
                        "FROM (%s) " +
                        "GROUP BY TUMBLE(rowtime, INTERVAL '20' SECOND)",
                joinQuery);

        System.out.println(finalAgg);
        // get Table for SQL query
        Table result = tEnv.sqlQuery(finalAgg);
        // convert Table into append-only DataStream
        DataStream<Row> resultStream =
                tEnv.toAppendStream(result, Types.ROW(Types.INT, Types.SQL_TIMESTAMP));

        final StreamingFileSink<Row> sink = StreamingFileSink
                .forRowFormat(new Path(outputPath), (Encoder<Row>) (element, stream) -> {
                    PrintStream out = new PrintStream(stream);
                    out.println(element.toString());
                })
                .withBucketAssigner(new KeyBucketAssigner())
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        resultStream.print();

        resultStream
                // inject a KillMapper that forwards all records but terminates the first execution attempt
                .map(new KillMapper()).setParallelism(1)
                // add sink function
                .addSink(sink).setParallelism(1);

        sEnv.execute();
    }

    /**
     * Use first field for buckets.
     */
    public static final class KeyBucketAssigner implements BucketAssigner<Row, String> {

        private static final long serialVersionUID = 987325769970523326L;

        @Override
        public String getBucketId(final Row element, final Context context) {
            return String.valueOf(element.getField(0));
        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }
    }

    /**
     * Kills the first execution attempt of an application when it receives the second record.
     */
    public static class KillMapper implements MapFunction<Row, Row>, ListCheckpointed<Integer>, ResultTypeQueryable {

        // counts all processed records of all previous execution attempts
        private int saveRecordCnt = 0;
        // counts all processed records of this execution attempt
        private int lostRecordCnt = 0;

        @Override
        public Row map(Row value) {

            // the both counts are the same only in the first execution attempt
            if (saveRecordCnt == 1 && lostRecordCnt == 1) {
                throw new RuntimeException("Kill this Job!");
            }

            // update checkpointed counter
            saveRecordCnt++;
            // update non-checkpointed counter
            lostRecordCnt++;

            // forward record
            return value;
        }

        @Override
        public TypeInformation getProducedType() {
            return Types.ROW(Types.INT, Types.SQL_TIMESTAMP);
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) {
            return Collections.singletonList(saveRecordCnt);
        }

        @Override
        public void restoreState(List<Integer> state) {
            for (Integer i : state) {
                saveRecordCnt += i;
            }
        }
    }
}
