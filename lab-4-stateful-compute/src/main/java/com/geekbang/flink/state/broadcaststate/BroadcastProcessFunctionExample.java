package com.geekbang.flink.state.broadcaststate;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/**
 *  Custom BroadcastProcessFunction Example
 */
public class BroadcastProcessFunctionExample {

    public static void main(String[] args) throws Exception {

        final MapStateDescriptor<Long, String> utterDescriptor = new MapStateDescriptor<>(
                "broadcast-state", BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
        );

        final Map<Long, String> expected = new HashMap<>();
        expected.put(0L, "test:0");
        expected.put(1L, "test:1");
        expected.put(2L, "test:2");
        expected.put(3L, "test:3");
        expected.put(4L, "test:4");
        expected.put(5L, "test:5");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final DataStream<Long> srcOne = env.generateSequence(0L, 5L)
                .assignTimestampsAndWatermarks(new CustomWmEmitter<Long>() {

                    private static final long serialVersionUID = -8500904795760316195L;

                    @Override
                    public long extractTimestamp(Long element, long previousElementTimestamp) {
                        return element;
                    }
                });

        final DataStream<String> srcTwo = env.fromCollection(expected.values())
                .assignTimestampsAndWatermarks(new CustomWmEmitter<String>() {

                    private static final long serialVersionUID = -2148318224248467213L;

                    @Override
                    public long extractTimestamp(String element, long previousElementTimestamp) {
                        return Long.parseLong(element.split(":")[1]);
                    }
                });

        final BroadcastStream<String> broadcast = srcTwo.broadcast(utterDescriptor);

        // the timestamp should be high enough to trigger the timer after all the elements arrive.
        final DataStream<String> output = srcOne.connect(broadcast).process(
                new CustomBroadcastProcessFunction());

        output.print().setParallelism(1);

        env.execute();

    }

    private abstract static class CustomWmEmitter<T> implements AssignerWithPunctuatedWatermarks<T> {

        private static final long serialVersionUID = -5187335197674841233L;

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(T lastElement, long extractedTimestamp) {
            return new Watermark(extractedTimestamp);
        }
    }

    /**
     * This doesn't do much but we use it to verify that translation of non-keyed broadcast connect
     * works.
     */
    private static class CustomBroadcastProcessFunction extends
            BroadcastProcessFunction<Long, String, String> {

        private static final long serialVersionUID = 7616910653561100842L;

        private transient MapStateDescriptor<Long, String> descriptor;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            descriptor = new MapStateDescriptor<>(
                    "broadcast-state", BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
            );
        }

        @Override
        public void processElement(Long value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
        }

        @Override
        public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
            long key = Long.parseLong(value.split(":")[1]);
            ctx.getBroadcastState(descriptor).put(key, value);
        }
    }
}