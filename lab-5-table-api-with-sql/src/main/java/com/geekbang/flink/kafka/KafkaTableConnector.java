package com.geekbang.flink.kafka;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.table.sinks.CsvTableSink;


public class KafkaTableConnector {

    public static void main(String args[]) throws Exception {

        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        //注册Kafka Table数据源
        bsTableEnv.connect(
                new Kafka()
                        .version("0.11")
                        .topic("order-input")
                        .startFromEarliest()
                        .property("zookeeper.connect", "172.27.133.19:7181")
                        .property("bootstrap.servers", "172.27.133.19:9092")
        ).withFormat(new Json().jsonSchema(
                "{" +
                        "  \"definitions\": {" +
                        "    \"orderItems\": {" +
                        "      \"type\": \"object\"," +
                        "      \"properties\": {" +
                        "        \"orderItemId\": {" +
                        "          \"type\": \"string\"" +
                        "        }," +
                        "        \"price\": {" +
                        "          \"type\": \"number\"" +
                        "        }," +
                        "        \"itemLinkIds\": {" +
                        "          \"type\": \"object\"," +
                        "          \"properties\": {" +
                        "            \"linkId\": {" +
                        "              \"type\": \"string\"" +
                        "            }," +
                        "            \"price\": {" +
                        "              \"type\": \"number\"" +
                        "            }" +
                        "          }," +
                        "          \"required\": [" +
                        "            \"linkId\"," +
                        "            \"price\"" +
                        "          ]" +
                        "        }" +
                        "      }," +
                        "      \"required\": [" +
                        "        \"orderItemId\"," +
                        "        \"price\"" +
                        "      ]" +
                        "    }" +
                        "  }," +
                        "  \"type\": \"object\"," +
                        "  \"properties\": {" +
                        "    \"transactionId\": {" +
                        "      \"type\": \"string\"" +
                        "    }," +
                        "    \"orderTime\": {" +
                        "      \"type\": \"string\"," +
                        "      \"format\": \"date-time\"" +
                        "    }," +
                        "    \"orderItems\": {" +
                        "      \"oneOf\": [" +
                        "        {" +
                        "          \"type\": \"null\"" +
                        "        }," +
                        "        {" +
                        "          \"$ref\": \"#/definitions/orderItems\"" +
                        "        }" +
                        "      ]" +
                        "    }" +
                        "  }" +
                        "}"
        )).withSchema(
                new Schema().field("transactionId", Types.STRING)
                        .field("orderTime", Types.SQL_TIMESTAMP)
                        .field("promiseTime", Types.SQL_TIMESTAMP)
                        .field("", Types.ROW_NAMED(new String[]{"linkids", "systemIds", "type"},
                                new TypeInformation[]{Types.STRING, Types.STRING, Types.BIG_DEC}))
        ).inAppendMode().createTemporaryTable("order_table");

        String path = "/Users/zhanglibing/Desktop/learning-flink/data/table/order_result.csv";
        CsvTableSink sink = new CsvTableSink(
                path,
                "|",
                1,
                FileSystem.WriteMode.OVERWRITE);

        bsTableEnv.registerTableSink(
                "result_table",
                // specify table schema
                new String[]{"transactionId", "orderTime", "promiseTime", "orderAmount"},
                new TypeInformation[]{Types.STRING, Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP, Types.BIG_DEC},
                sink);

        bsTableEnv.sqlUpdate("insert into result_table(transactionId,userCode,couponCode) select * from order_table");

        bsTableEnv.execute("Kafka Table Connector Test");
    }
}
