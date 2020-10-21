package com.geekbang.flink.state.operatorstate;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;

import java.util.Collections;
import java.util.List;

public class MapWhitListCheckpointedExample {

    public static void main(String[] args) {

    }

   static class CountingFunction<T> implements MapFunction<T, Tuple2<T, Long>>, ListCheckpointed<Long> {

        private long count;

        @Override
        public List<Long> snapshotState(long checkpointId, long timestamp) {
            // return a single element - our count
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Long> state) throws Exception {
            for (Long l : state) {
                count += l;
            }
        }

        @Override
        public Tuple2<T, Long> map(T value) {
            count++;
            return new Tuple2<>(value, count);
        }
    }

}
