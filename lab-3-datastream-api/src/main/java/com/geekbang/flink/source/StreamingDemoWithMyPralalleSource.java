package com.geekbang.flink.source;

import com.geekbang.flink.source.custom.CustomParallelSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 使用多并行度的source
 */
public class StreamingDemoWithMyPralalleSource {

    public static void main(String[] args) throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据源
        DataStreamSource<Tuple2<String, Long>> text = env.addSource(new CustomParallelSource()).setParallelism(5);

        DataStream<Long> num = text.map(new MapFunction<Tuple2<String, Long>, Long>() {
            @Override
            public Long map(Tuple2<String, Long> input) throws Exception {
                System.out.println("接收到数据：" + "ThreadName: " + input.f0 + " ,Value:" + input.f1);
                return input.f1;
            }
        });

        //每2秒钟处理一次数据
        DataStream<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);

        //打印结果
        sum.print().setParallelism(1);

        String jobName = StreamingDemoWithMyPralalleSource.class.getSimpleName();
        env.execute(jobName);
    }
}
