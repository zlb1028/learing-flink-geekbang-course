package com.geekbang.flink.state.keyedstate;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;


public class MapWithCounter {
    public static void main(String[] args) {

    }
}

class MapWithCounterFunction extends RichMapFunction<Tuple2<String, String>, Long> {

    private ValueState<Long> totalLengthByKey;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Long> stateDescriptor =
                new ValueStateDescriptor<Long>("sum of length", LongSerializer.INSTANCE);
        totalLengthByKey = getRuntimeContext().getState(stateDescriptor);
    }

    @Override
    public Long map(Tuple2<String, String> value) throws Exception {
        Long length = totalLengthByKey.value();
        if (length == null) {
            length = 0L;
        }
        long newTotalLength = length + value.f1.length();
        totalLengthByKey.update(newTotalLength);
        return newTotalLength;
    }
}

