package cep;

import cep.domain.Rule;
import cep.domain.SceneOutData;
import cep.util.PatternUtil;
import com.alibaba.fastjson.JSON;
import individualAssessment1.domain.Event;
import individualAssessment1.util.KafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class CepMain {

    public static void main(String[] args) throws Exception {

//        cepTest();
        doCepApp();
        return;

    }
    private static void doCepApp() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1 分钟一次CheckPoint
//        env.enableCheckpointing(TimeUnit.MINUTES.toMillis(1));
//        env.setParallelism(1);
        Properties props = getProperties();

        SingleOutputStreamOperator<Event> map = env.addSource(new FlinkKafkaConsumer011<>(
                KafkaUtil.TOPIC, new SimpleStringSchema(),
                props
        )).map(string -> JSON.parseObject(string, Event.class));


        // 加载模式
        Map<Long, Pattern> sidPatternMap = PatternUtil.sidPatternMap;

        Set<Map.Entry<Long, Pattern>> sidPatternMapEntries = sidPatternMap.entrySet();

        for (Map.Entry<Long, Pattern> sidPatternMapEntry : sidPatternMapEntries) {
            Long sceneId = sidPatternMapEntry.getKey();
            Pattern pattern = sidPatternMapEntry.getValue();

            // Create a pattern stream from our warning pattern
            // 通过模式从数据源生成模式流（目前包含数据源的全部数据）
            PatternStream<Event> tempPatternStream = CEP.pattern(
                    map.keyBy(new KeySelector<Event, String>() {
                        @Override
                        public String getKey(Event event) throws Exception {
                            return (String) event.getPayload().get("member_id");
                        }
                    }),
                    pattern);

            // Generate temperature warnings for each matched warning pattern
            // 通过模式流，选择出匹配温度告警模式的数据流。
            SingleOutputStreamOperator<SceneOutData> first = tempPatternStream.select(new PatternSelectFunction<Event, SceneOutData>() {
                @Override
                public SceneOutData select(Map<String, List<Event>> map) throws Exception {
                    Event first = map.get("1").get(0);

                    System.out.println("map-1:"+map.get("1").size()+"  " +map.get("1"));
                    System.out.println("map-2:"+map.get("2").size()+"  " +map.get("2"));


                    SceneOutData sceneOutData = new SceneOutData();
                    sceneOutData.setSceneId(sceneId);
                    sceneOutData.setMemberId((String) first.getPayload().get("member_id"));


                    return sceneOutData;
                }
            });

            first.print();
        }





        env.execute("cep");
    }

    private static void cepTest() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1 分钟一次CheckPoint
//        env.enableCheckpointing(TimeUnit.MINUTES.toMillis(1));
//        env.setParallelism(1);
        Properties props = getProperties();

        SingleOutputStreamOperator<Event> map = env.addSource(new FlinkKafkaConsumer011<>(
                // kafka topic， String 序列化
                KafkaUtil.TOPIC, new SimpleStringSchema(),
                props
        )).map(string -> JSON.parseObject(string, Event.class));




//        Pattern<Event, Event> pattern = PatternUtil.createPattern();


        Pattern<Event, Event> pattern = Pattern.<Event>begin("1")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getPayload().get("type").equals(Rule.TYPE_LOGIN);
                    }
                })
                .within(Time.seconds(1))
                .next("2")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getPayload().get("type").equals(Rule.TYPE_PAY);
                    }
                });


        // Create a pattern stream from our warning pattern
        // 通过模式从数据源生成模式流（目前包含数据源的全部数据）
        PatternStream<Event> tempPatternStream = CEP.pattern(
                map.keyBy(new KeySelector<Event, String>() {
                    @Override
                    public String getKey(Event event) throws Exception {
                        return (String) event.getPayload().get("member_id");
                    }
                }),
                pattern);

        // Generate temperature warnings for each matched warning pattern
        // 通过模式流，选择出匹配温度告警模式的数据流。
        SingleOutputStreamOperator<Event> first = tempPatternStream.select(new PatternSelectFunction<Event, Event>() {
            @Override
            public Event select(Map<String, List<Event>> map) throws Exception {
                Event first = map.get("1").get(0);

                System.out.println("map-1:"+map.get("1").size()+"  " +map.get("1"));
                System.out.println("map-2:"+map.get("2").size()+"  " +map.get("2"));
                return first;
            }
        });

//        first.print();


        env.execute("cep");
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //key 反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest"); //value 反序列化
        return props;
    }
}
