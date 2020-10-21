package com.geekbang.flink.state.broadcaststate;

import com.geekbang.flink.state.broadcaststate.model.Color;
import com.geekbang.flink.state.broadcaststate.model.Item;
import com.geekbang.flink.state.broadcaststate.model.Rule;
import com.geekbang.flink.state.broadcaststate.model.Shape;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KeyedBroadcastProcessFunctionExample {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Item> itemStream = env.fromElements(
                new Item(),
                new Item()
        );

        DataStream<Rule> ruleStream = env.fromElements(
                new Rule(),
                new Rule()
        );

        // key the items by color
        KeyedStream<Item, Color> colorPartitionedStream = itemStream
                .keyBy(new KeySelector<Item, Color>() {
                    @Override
                    public Color getKey(Item item) throws Exception {
                        return item.getColor();
                    }
                });

        // a map descriptor to store the name of the rule (string) and the rule itself.
        MapStateDescriptor<String, Rule> ruleStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<Rule>() {
                }));

        // broadcast the rules and create the broadcast state
        BroadcastStream<Rule> ruleBroadcastStream = ruleStream
                .broadcast(ruleStateDescriptor);


        DataStream<String> output = colorPartitionedStream
                .connect(ruleBroadcastStream)
                .process(

                        // type arguments in our KeyedBroadcastProcessFunction represent:
                        //   1. the key of the keyed stream
                        //   2. the type of elements in the non-broadcast side
                        //   3. the type of elements in the broadcast side
                        //   4. the type of the result, here a string

                        new KeyedBroadcastProcessFunction<Color, Item, Rule, String>() {

                            // store partial matches, i.e. first elements of the pair waiting for their second element
                            // we keep a list as we may have many first elements waiting
                            private final MapStateDescriptor<String, List<Item>> mapStateDesc =
                                    new MapStateDescriptor<>(
                                            "items",
                                            BasicTypeInfo.STRING_TYPE_INFO,
                                            new ListTypeInfo<>(Item.class));

                            // identical to our ruleStateDescriptor above
                            private final MapStateDescriptor<String, Rule> ruleStateDescriptor =
                                    new MapStateDescriptor<>(
                                            "RulesBroadcastState",
                                            BasicTypeInfo.STRING_TYPE_INFO,
                                            TypeInformation.of(new TypeHint<Rule>() {
                                            }));

                            @Override
                            public void processBroadcastElement(Rule value,
                                                                Context ctx,
                                                                Collector<String> out) throws Exception {
                                ctx.getBroadcastState(ruleStateDescriptor).put(value.name, value);
                            }

                            @Override
                            public void processElement(Item value,
                                                       ReadOnlyContext ctx,
                                                       Collector<String> out) throws Exception {

                                final MapState<String, List<Item>> state = getRuntimeContext().getMapState(mapStateDesc);
                                final Shape shape = value.getShape();

                                for (Map.Entry<String, Rule> entry :
                                        ctx.getBroadcastState(ruleStateDescriptor).immutableEntries()) {
                                    final String ruleName = entry.getKey();
                                    final Rule rule = entry.getValue();

                                    List<Item> stored = state.get(ruleName);
                                    if (stored == null) {
                                        stored = new ArrayList<>();
                                    }

                                    if (shape == rule.second && !stored.isEmpty()) {
                                        for (Item i : stored) {
                                            out.collect("MATCH: " + i + " - " + value);
                                        }
                                        stored.clear();
                                    }

                                    // there is no else{} to cover if rule.first == rule.second
                                    if (shape.equals(rule.first)) {
                                        stored.add(value);
                                    }

                                    if (stored.isEmpty()) {
                                        state.remove(ruleName);
                                    } else {
                                        state.put(ruleName, stored);
                                    }
                                }
                            }
                        }
                        // my matching logic
                );

        output.print();
        env.execute();

    }
}