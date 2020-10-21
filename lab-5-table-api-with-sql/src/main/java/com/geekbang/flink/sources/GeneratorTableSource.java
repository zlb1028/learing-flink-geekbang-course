package com.geekbang.flink.sources;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedFieldMapping;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class GeneratorTableSource implements StreamTableSource<Row>, DefinedRowtimeAttributes, DefinedFieldMapping {

    private final int numKeys;
    private final float recordsPerKeyAndSecond;
    private final int durationSeconds;
    private final int offsetSeconds;

    public GeneratorTableSource(int numKeys, float recordsPerKeyAndSecond, int durationSeconds, int offsetSeconds) {
        this.numKeys = numKeys;
        this.recordsPerKeyAndSecond = recordsPerKeyAndSecond;
        this.durationSeconds = durationSeconds;
        this.offsetSeconds = offsetSeconds;
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        return execEnv.addSource(new Generator(numKeys, recordsPerKeyAndSecond, durationSeconds, offsetSeconds));
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return Types.ROW(Types.INT, Types.LONG, Types.STRING);
    }

    @Override
    public TableSchema getTableSchema() {
        return new TableSchema(
                new String[]{"key", "rowtime", "payload"},
                new TypeInformation[]{Types.INT, Types.SQL_TIMESTAMP, Types.STRING});
    }

    @Override
    public String explainSource() {
        return "GeneratorTableSource";
    }

    @Override
    public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
        return Collections.singletonList(
                new RowtimeAttributeDescriptor(
                        "rowtime",
                        new ExistingField("ts"),
                        new BoundedOutOfOrderTimestamps(100)));
    }

    @Override
    public Map<String, String> getFieldMapping() {
        Map<String, String> mapping = new HashMap<>();
        mapping.put("key", "f0");
        mapping.put("ts", "f1");
        mapping.put("payload", "f2");
        return mapping;
    }

    /**
     * Data-generating source function.
     */
    @Slf4j
    public static class Generator implements SourceFunction<Row>, ResultTypeQueryable<Row>, ListCheckpointed<Long> {

        private final int numKeys;
        private final int offsetSeconds;

        private final int sleepMs;
        private final int durationMs;

        private long ms = 0;

        public Generator(int numKeys, float rowsPerKeyAndSecond, int durationSeconds, int offsetSeconds) {
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
                    for (int i = 0; i < numKeys; i++) {
                        log.info(Row.of(i, ms + offsetMS, "Some payload...").toString());
                        ctx.collect(Row.of(i, System.currentTimeMillis(), "Some payload..."));
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
            return Types.ROW(Types.INT, Types.LONG, Types.STRING);
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