package com.geekbang.flink;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;


public class RowRegisterRowWithNames {

    public static void main(String args[]) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        List<Row> data = new ArrayList<>();
        data.add(Row.of(1, 1L, "Hi"));
        data.add(Row.of(2, 2L, "Hello"));
        data.add(Row.of(3, 2L, "Hello world"));

        TypeInformation<?>[] types = {
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO};

        String[] names = {"a", "b", "c"};

        RowTypeInfo typeInfo = new RowTypeInfo(types, names);

        DataStream<Row> ds = env.fromCollection(data).returns(typeInfo);

        Table in = tableEnv.fromDataStream(ds, "a,b,c");

        tableEnv.registerTable("MyTableRow", in);

        String sqlQuery = "SELECT a,c FROM MyTableRow";

        Table result = tableEnv.sqlQuery(sqlQuery);

        DataStream<Row> resultSet = tableEnv.toAppendStream(result, Row.class);

        resultSet.print();

        env.execute();

    }
}



