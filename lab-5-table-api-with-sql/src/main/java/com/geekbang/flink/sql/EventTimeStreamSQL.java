/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.geekbang.flink.sql;

import com.geekbang.flink.sources.EventTimeTableSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end test for Stream SQL queries.
 *
 * <p>Includes the following SQL features:
 * - OVER window aggregation
 * - keyed and non-keyed GROUP BY TUMBLE aggregation
 * - windowed INNER JOIN
 * - TableSource with event-time attribute
 *
 * <p>The stream is bounded and will complete after about a minute.
 * The result is always constant.
 * The job is killed on the first attempt and restarted.
 *
 * <p>Parameters:
 * -outputPath Sets the path to where the result data is written.
 */
public class EventTimeStreamSQL {

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        String outputPath = params.getRequired("outputPath");

        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        sEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3,
                Time.of(10, TimeUnit.SECONDS)
        ));
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        sEnv.enableCheckpointing(4000);

        sEnv.getConfig().setAutoWatermarkInterval(1000);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv);

        tEnv.registerTableSource("table1", new EventTimeTableSource("table1", 5, 0.2f, 60, 0));

        tEnv.registerTableSource("table2", new EventTimeTableSource("table2", 1, 0.2f, 60, 5));

        int overWindowSizeSeconds = 5;

        int tumbleWindowSizeSeconds = 10;

        String overQuery = String.format(
                "SELECT " +
                        "  key, " +
                        "  rowtime, " +
                        "  COUNT(*) OVER (PARTITION BY key ORDER BY rowtime RANGE BETWEEN INTERVAL '%d' SECOND PRECEDING AND CURRENT ROW) AS cnt " +
                        "FROM table1",
                overWindowSizeSeconds);

//        System.out.println("OVERQUERY： " + overQuery);
//
//        Table overWindowTable = tEnv.sqlQuery(overQuery);
//
//        DataStream<Row> overWindowStream = tEnv.toAppendStream(overWindowTable, Types.ROW(Types.INT, Types.SQL_TIMESTAMP, Types.LONG));
//
//        overWindowStream.print();


//        String tumbleQuery = String.format(
//                "SELECT " +
//                        "  key, " +
//                        "  CASE SUM(cnt) / COUNT(*) WHEN 101 THEN 1 ELSE 99 END AS correct, " +
//                        "  TUMBLE_START(rowtime, INTERVAL '%d' SECOND) AS wStart, " +
//                        "  TUMBLE_ROWTIME(rowtime, INTERVAL '%d' SECOND) AS rowtime, " +
//                        "  TUMBLE_END(rowtime, INTERVAL '%d' SECOND) AS wEnd " +
//                        "FROM (%s) " +
//                        "WHERE rowtime > TIMESTAMP '1970-01-01 00:00:01' " +
//                        "GROUP BY key, TUMBLE(rowtime, INTERVAL '%d' SECOND)",
//                tumbleWindowSizeSeconds,
//                tumbleWindowSizeSeconds,
//                tumbleWindowSizeSeconds,
//                overQuery,
//                tumbleWindowSizeSeconds);

        String tumbleQuery = String.format(
                "SELECT " +
                        "  key, " +
                        "  COUNT(*) AS count_num, " +
                        "  TUMBLE_START(rowtime, INTERVAL '%d' SECOND) AS wStart, " +
                        "  TUMBLE_ROWTIME(rowtime, INTERVAL '%d' SECOND) AS eventTime, " +
                        "  TUMBLE_END(rowtime, INTERVAL '%d' SECOND) AS wEnd " +
                        "FROM table2 " +
                        "WHERE rowtime > TIMESTAMP '1970-01-01 00:00:01' " +
                        "GROUP BY key, TUMBLE(rowtime, INTERVAL '%d' SECOND)",
                tumbleWindowSizeSeconds,
                tumbleWindowSizeSeconds,
                tumbleWindowSizeSeconds,
                tumbleWindowSizeSeconds);

        System.out.println("TUMBLEQUERY： " + tumbleQuery);

        Table tumpleWindowTable = tEnv.sqlQuery(tumbleQuery);

        DataStream<Row> overWindowStream = tEnv.toAppendStream(tumpleWindowTable, Types.ROW(Types.INT, Types.LONG, Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP));

        overWindowStream.print();

//        String joinQuery = String.format(
//                "SELECT " +
//                        "  t1.key, " +
//                        "  t2.rowtime AS rowtime, " +
//                        "  t2.correct," +
//                        "  t2.wStart " +
//                        "FROM table2 t1, (%s) t2 " +
//                        "WHERE " +
//                        "  t1.key = t2.key AND " +
//                        "  t1.rowtime BETWEEN t2.rowtime AND t2.rowtime + INTERVAL '%d' SECOND",
//                tumbleQuery,
//                tumbleWindowSizeSeconds);
//
//        System.out.println("JOINQUERY： " + joinQuery);
//
//
//        String finalAgg = String.format(
//                "SELECT " +
//                        "  SUM(correct) AS correct, " +
//                        "  TUMBLE_START(rowtime, INTERVAL '20' SECOND) AS rowtime " +
//                        "FROM (%s) " +
//                        "GROUP BY TUMBLE(rowtime, INTERVAL '20' SECOND)",
//                joinQuery);
//
//        System.out.println("FINALAGG： " + finalAgg);
//
//        // get Table for SQL query
//        Table result = tEnv.sqlQuery(finalAgg);
//        // convert Table into append-only DataStream
//        DataStream<Row> resultStream =
//                tEnv.toAppendStream(result, Types.ROW(Types.INT, Types.SQL_TIMESTAMP));
//
//        resultStream.print();
//
//        final StreamingFileSink<Row> sink = StreamingFileSink
//                .forRowFormat(new Path(outputPath), (Encoder<Row>) (element, stream) -> {
//                    PrintStream out = new PrintStream(stream);
//                    out.println(element.toString());
//                })
//                .withBucketAssigner(new KeyBucketAssigner())
//                .withRollingPolicy(OnCheckpointRollingPolicy.build())
//                .build();
//
//        resultStream
//                // inject a KillMapper that forwards all records but terminates the first execution attempt
//                .map(new KillMapper()).setParallelism(1)
//                // add sink function
//                .addSink(sink).setParallelism(1);

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
