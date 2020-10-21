package com.geekbang.flink.windows;

import com.geekbang.flink.sources.OrderSourceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


public class Windows {
    public static void main(String args[]) throws Exception {

        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(sEnv);
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        sEnv.enableCheckpointing(4000);
        sEnv.getConfig().setAutoWatermarkInterval(1000);

        // ingest a DataStream from an external source
        DataStream<Tuple4<Long, String, Integer, Long>> ds = sEnv.addSource(new OrderSourceFunction(10, 0.01f, 60, 0));
        // register the DataStream as table "Orders"
        tableEnv.registerDataStream("Orders", ds, "user, product, amount, rowtime.rowtime, proctime.proctime");
        ds.print();
        // compute SUM(amount) per day (in event-time)
        Table result1 = tableEnv.sqlQuery(
                "SELECT user, " +
                        "  TUMBLE_START(rowtime, INTERVAL '5' SECOND) as wStart,  " +
                        "  SUM(amount) FROM Orders " +
                        "GROUP BY TUMBLE(rowtime, INTERVAL '5' SECOND), user");

        // compute SUM(amount) per day (in processing-time)
        Table result2 = tableEnv.sqlQuery(
                "SELECT user, SUM(amount) FROM Orders GROUP BY TUMBLE(proctime, INTERVAL '1' DAY), user");

        // compute every hour the SUM(amount) of the last 24 hours in event-time
        Table result3 = tableEnv.sqlQuery(
                "SELECT product, SUM(amount) FROM Orders GROUP BY HOP(rowtime, INTERVAL '1' HOUR, INTERVAL '1' DAY), product");

        // compute SUM(amount) per session with 12 hour inactivity gap (in event-time)
        Table result4 = tableEnv.sqlQuery(
                "SELECT user, " +
                        "  SESSION_START(rowtime, INTERVAL '12' HOUR) AS sStart, " +
                        "  SESSION_ROWTIME(rowtime, INTERVAL '12' HOUR) AS snd, " +
                        "  SUM(amount) " +
                        "FROM Orders " +
                        "GROUP BY SESSION(rowtime, INTERVAL '12' HOUR), user");

        DataStream<Row> ds1 = tableEnv.toAppendStream(result1, Types.ROW(Types.LONG, Types.SQL_TIMESTAMP, Types.INT));
        ds1.print();
        sEnv.execute();
    }
}
