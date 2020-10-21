package com.geekbang.flink.source.custom;

import org.apache.flink.api.common.accumulators.AverageAccumulator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 自定义实现一个支持并行度的source
 */
public class CustomRichParallelSource extends RichParallelSourceFunction<Long> {

    private long count = 1L;

    private boolean isRunning = true;

    /**
     * 启动数据源读取线程
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while(isRunning){
            ctx.collect(count);
            count++;
            this.getRuntimeContext().getAccumulator("accumulator").add(count);
            //每秒产生一条数据
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    /**
     * 只在启动过程中被调用一次,实现对Function中的状态初始化
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("Executing the open method: " + "ThreadName: " + Thread.currentThread().getName());
        this.getRuntimeContext().addAccumulator("accumulator",new AverageAccumulator());
        super.open(parameters);
    }

    /**
     * 实现关闭链接的代码
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
    }
}
