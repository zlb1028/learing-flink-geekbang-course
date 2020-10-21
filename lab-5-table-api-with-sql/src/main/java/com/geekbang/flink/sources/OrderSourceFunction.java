package com.geekbang.flink.sources;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.List;

@Slf4j
public class OrderSourceFunction implements SourceFunction<Tuple4<Long, String, Integer, Long>>, ListCheckpointed<Long> {

    private final int numKeys;
    private final int offsetSeconds;
    private final int sleepMs;
    private final int durationMs;
    private long ms = 0;

    private String[] products = new String[]{"PC","Gucci","Channel","YSL"};

    public OrderSourceFunction(int numKeys, float rowsPerKeyAndSecond, int durationSeconds, int offsetSeconds) {
        this.numKeys = numKeys;
        this.durationMs = durationSeconds * 1000;
        this.offsetSeconds = offsetSeconds;

        this.sleepMs = (int) (1000 / rowsPerKeyAndSecond);
    }

    @Override
    public void run(SourceContext<Tuple4<Long, String, Integer, Long>> ctx) throws Exception {
        long offsetMS = offsetSeconds * 2000L;

        while (ms < durationMs) {
            synchronized (ctx.getCheckpointLock()) {
                for (int i = 0; i < numKeys; i++) {
                    log.info(Row.of(i, "product", new java.util.Random().nextInt(100), ms + offsetMS).toString());
                    ctx.collect(new Tuple4<Long, String, Integer, Long>(i + 0L, products[i % 4], Integer.valueOf(new java.util.Random().nextInt(100)),System.currentTimeMillis()));
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