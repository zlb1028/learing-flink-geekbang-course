package com.geekbang.flink.types;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.sql.DataSource;


public class PojosClass {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
        TypeInformation<String> info = TypeInformation.of(String.class);
//
//        TypeInformation<Tuple2<String, Double>> info2 = TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {
//        });
//
//        TypeInformation<Long> longTypeInfo = BasicTypeInfo.LONG_TYPE_INFO;

        DataStream<Integer> input = env.fromElements(1, 2, 3);

        DataStream<Tuple2<Integer, Long>> output = input.map(new AppendOne<>()).returns(TypeInformation.of(new TypeHint<Tuple2<Integer, Long>>() {
        }));

        output.print();

        env.execute("PojosClass");

    }


    static class AppendOne<T> implements MapFunction<T, Tuple2<T, Long>> {
        @Override
        public Tuple2<T, Long> map(T value) {
            return new Tuple2<T, Long>(value, 1L);
        }
    }

}
