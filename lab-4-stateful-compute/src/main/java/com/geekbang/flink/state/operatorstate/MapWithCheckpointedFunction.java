package com.geekbang.flink.state.operatorstate;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;


public class MapWithCheckpointedFunction {

    public static void main(String[] args) {

    }

    static class CustomMapFunction<T> implements MapFunction<T, T>, CheckpointedFunction {

        private ReducingState<Long> countPerKey;

        private ListState<Long> countPerPartition;

        private long localCount;

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

            countPerKey = context.getKeyedStateStore().getReducingState(
                    new ReducingStateDescriptor<>("perKeyCount", new AddFunction(), Long.class));

            countPerPartition = context.getOperatorStateStore().getUnionListState(
                    new ListStateDescriptor<>("perPartitionCount", Long.class));
            for (Long l : countPerPartition.get()) {
                localCount += l;
            };
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            countPerPartition.clear();
            countPerPartition.add(localCount);
        }

        @Override
        public T map(T value) throws Exception {
            countPerKey.add(1L);
            localCount++;
            return value;
        }
    }

    static class AddFunction implements ReduceFunction<Long> {
        @Override
        public Long reduce(Long aLong, Long t1) throws Exception {
            return null;
        }
    }
}


