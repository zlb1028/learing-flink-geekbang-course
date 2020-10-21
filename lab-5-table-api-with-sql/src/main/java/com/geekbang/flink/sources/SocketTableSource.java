package com.geekbang.flink.sources;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedFieldMapping;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SocketTableSource implements StreamTableSource<Row>, DefinedRowtimeAttributes, DefinedFieldMapping {

    private String hostname;
    private int port;

    public SocketTableSource(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
    }

    @Nullable
    @Override
    public Map<String, String> getFieldMapping() {
        Map<String, String> mapping = new HashMap<>();
        mapping.put("key", "f0");
        mapping.put("ts", "f1");
        mapping.put("payload", "f2");
        return mapping;
    }

    @Override
    public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
        return Collections.singletonList(
                new RowtimeAttributeDescriptor(
                        "rowtime",
                        new ExistingField("ts"),
                        new BoundedOutOfOrderTimestamps(100)));
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        DataStream<String> ds = execEnv.socketTextStream(hostname, port);
        DataStream<Row> data = ds.map(new MapFunction<String, Row>() {
                                          @Override
                                          public Row map(String str) throws Exception {
                                              String[] fields = str.split(",");
                                              return Row.of(Integer.parseInt(fields[0]), Long.parseLong(fields[1]), "Some payload...");
                                          }
                                      }
        );
        return data;
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return Types.ROW(Types.INT, Types.LONG, Types.STRING);
    }

    @Override
    public TableSchema getTableSchema() {
        return new TableSchema(new String[]{"key", "rowtime", "payload"}, new TypeInformation[]{Types.INT, Types.SQL_TIMESTAMP, Types.STRING});
    }
}
