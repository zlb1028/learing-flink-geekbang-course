package com.geekbang.flink.sources;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedFieldMapping;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ProcessTimeTableSource implements StreamTableSource<Row>, DefinedProctimeAttribute, DefinedFieldMapping {

    private final String sourceTag;
    private final int numKeys;
    private final float recordsPerKeyAndSecond;
    private final int durationSeconds;
    private final int offsetSeconds;

    public ProcessTimeTableSource(String sourceTag, int numKeys, float recordsPerKeyAndSecond, int durationSeconds, int offsetSeconds) {
        this.sourceTag = sourceTag;
        this.numKeys = numKeys;
        this.recordsPerKeyAndSecond = recordsPerKeyAndSecond;
        this.durationSeconds = durationSeconds;
        this.offsetSeconds = offsetSeconds;
    }

    @Nullable
    @Override
    public String getProctimeAttribute() {
        return "proctime";
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        return execEnv.addSource(new DataGenerator(sourceTag, numKeys, recordsPerKeyAndSecond, durationSeconds, offsetSeconds));
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return Types.ROW(Types.INT, Types.STRING);
    }

    @Override
    public TableSchema getTableSchema() {
        String[] fields = new String[]{"key", "payload", "proctime"};
        TypeInformation[] typeInformation = new TypeInformation[]{Types.INT, Types.STRING, Types.SQL_TIMESTAMP};
        TableSchema tableSchema = new TableSchema(fields, typeInformation);
        return tableSchema;
    }

    @Nullable
    @Override
    public Map<String, String> getFieldMapping() {
        Map<String, String> mapping = new HashMap<>();
        mapping.put("key", "f0");
        mapping.put("payload", "f1");
        mapping.put("proctime", "f2");
        return mapping;
    }

    public static class DataGenerator implements SourceFunction<Row>, ResultTypeQueryable<Row>, ListCheckpointed<Long> {

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
        public void run(SourceContext<Row> ctx) throws Exception {
            long offsetMS = offsetSeconds * 2000L;

            while (ms < durationMs) {
                synchronized (ctx.getCheckpointLock()) {
                    for (int i = 1; i <= numKeys; i++) {
                        ctx.collect(Row.of(i + 1000, sourceTag + " payload..."));
                        System.out.println("Table Source: " + sourceTag + "  PayLoad: " + Row.of(i + 1000, sourceTag + " payload...").toString());
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
        public TypeInformation<Row> getProducedType() {
            return Types.ROW(Types.INT, Types.STRING);
        }

        @Override
        public List<Long> snapshotState(long checkpointId, long timestamp) {
            return Collections.singletonList(ms);
        }

        @Override
        public void restoreState(List<Long> state) {
            for (Long l : state) {
                ms += l;
            }
        }
    }
}