package com.geekbang.flink.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.GroupWindowedTable;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


public class DataStreamToTable {

    public static void main(String args[]) throws Exception {

        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        sEnv.enableCheckpointing(4000);

        sEnv.getConfig().setAutoWatermarkInterval(1000);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv);

        DataStream inputStream = sEnv.addSource(new DataGenerator("table1", 5, 1000, 60, 0));

        inputStream.print();

        // declare an additional logical field as a processing time attribute
        Table table = tEnv.fromDataStream(inputStream, "key, event_time.rowtime, pay_load,process_time.proctime");

        GroupWindowedTable windowedTable = table.window(Tumble.over("10.minutes").on("process_time").as("userActionWindow"));

        DataStream dataStream = tEnv.toAppendStream(table, Types.ROW(Types.INT, Types.SQL_TIMESTAMP, Types.STRING, Types.SQL_TIMESTAMP));

        dataStream.print();

        sEnv.execute();

    }

    public static class DataGenerator implements SourceFunction, ResultTypeQueryable {

        private final String sourceTag;
        private final int numKeys;
        private final int offsetSeconds;
        private final int sleepMs;
        private final int durationMs;
        private long ms = 0;

        public DataGenerator(String sourceTag, int numKeys, float rowsPerKeyAndSecond, int durationSeconds, int offsetSeconds) {
            this.sourceTag = sourceTag;
            this.numKeys = numKeys;
            this.durationMs = durationSeconds * 1000;
            this.offsetSeconds = offsetSeconds;
            this.sleepMs = (int) (1000 / rowsPerKeyAndSecond);
        }

        @Override
        public void run(SourceContext ctx) throws Exception {
            long offsetMS = offsetSeconds * 2000L;

            while (ms < durationMs) {
                synchronized (ctx.getCheckpointLock()) {
                    for (int i = 1; i <= numKeys; i++) {
                        ctx.collect(Row.of(i + 1000, ms + offsetMS, sourceTag + " payload..."));
                        System.out.println("Table Source: " + sourceTag + "  PayLoad: " + Row.of(i + 1000, ms + offsetMS, sourceTag + " payload...").toString());
                    }
                    ms += sleepMs;
                }
                Thread.sleep(sleepMs);
            }
        }

        @Override
        public void cancel() {

        }

        @Override
        public TypeInformation getProducedType() {
            return Types.ROW(Types.INT, Types.SQL_TIMESTAMP, Types.STRING);
        }
    }
}
