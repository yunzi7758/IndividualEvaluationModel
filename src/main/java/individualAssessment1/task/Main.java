package individualAssessment1.task;

import com.alibaba.fastjson.JSON;
import individualAssessment1.domain.OutData;
import individualAssessment1.uitl.IndividualAssessment;
import individualAssessment1.domain.Event;
import individualAssessment1.domain.IaOutData;
import individualAssessment1.domain.Rule;
import individualAssessment1.map.CountScoreMap;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Desc: socket
 * Created by zhisheng on 2019-04-26
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class Main {
    public static void main(String[] args) throws Exception {

//        oneState(args);
        moreState();
        return;
    }

    private static void moreState() throws Exception {
    // 加载个体评估模型配置
        new IndividualAssessment().loadRules();


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1 分钟一次CheckPoint
        env.enableCheckpointing(TimeUnit.MINUTES.toMillis(1));
        env.setParallelism(2);
//
        CheckpointConfig checkpointConf = env.getCheckpointConfig();
        // CheckPoint 语义 EXACTLY ONCE
        checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties props = getProperties();


        System.out.println("=================start");

        DataStreamSource<String> appInfoSource = env.addSource(new FlinkKafkaConsumer011<>(
                // kafka topic， String 序列化
                individualAssessment1.util.KafkaUtil.TOPIC, new SimpleStringSchema(),
//                KafkaConfigUtil.buildKafkaProps( ExecutionEnvUtil.PARAMETER_TOOL))
                props
        ));


        SingleOutputStreamOperator<IaOutData> max = appInfoSource
                .keyBy(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String s) throws Exception {
                        Event event = JSON.parseObject(s, Event.class);

//                        System.out.println(s);
                        return (String) event.getPayload().get("member_id");
                    }
                })

                .map(new CountScoreMap())
                .flatMap(new FlatMapFunction<List<IaOutData>, IaOutData>() {
                    @Override
                    public void flatMap(List<IaOutData> iaOutData, Collector<IaOutData> collector) throws Exception {
                        for (IaOutData iaOutDatum : iaOutData) {
                            collector.collect(iaOutDatum);
                        }

                    }
                })

                .keyBy("iaid", "memberId")
                .timeWindow(Time.seconds(5)).max("score");


        // 定时调用sink，但是in数据还是逐条过来
        max.addSink(new SinkFunction<IaOutData>() {
            @Override
            public void invoke(IaOutData value, Context context) throws Exception {
                System.out.println(value);
            }
        });

        System.out.println("=================end");
        env.execute("Flink ia stat");
    }









    private static SingleOutputStreamOperator<Event> keyByModel(DataStreamSource<String> appInfoSource) {
        return appInfoSource

                .keyBy(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String s) throws Exception {
                        Event event = JSON.parseObject(s, Event.class);

//                        System.out.println(s);
                        return (String) event.getPayload().get("member_id");
                    }
                })
                .map(new MapFunction<String, Event>() {

                    @Override
                    public Event map(String s) throws Exception {
                        return JSON.parseObject(s, Event.class);
                    }
                })
                // 原数据流 已经根据member_id分流，
                // 下面将每个分支数据，又根据模型数，复制为多份。 最后使用.keyBy("iaid","memberId").timeWindow(Time.seconds(10)).max("score")
                // 这样有个问题，相同member_id流之间无法共享state，因为又根据模型分流了
//                .flatMap(new FlatMapFunction<Event, Event>() {
//                    @Override
//                    public void flatMap(Event event, Collector<Event> collector) throws Exception {
//
//                        for (Map.Entry<String, List<Rule>> stringListEntry : IndividualAssessment.idRulesMap.entrySet()) {
//                            event.getPayload().put("model_id", stringListEntry.getKey());
//                            collector.collect(event);
//                        }
//                    }
//                })
//                .keyBy(new KeySelector<Event, Event>() { // 上面flatMap分流之后，要keyby
//                    @Override
//                    public Event getKey(Event event) throws Exception {
//                        return event;
//                    }
//                })
//                .map(new CountScoreMap2())
//                .keyBy("iaid","memberId")
//                .timeWindow(Time.seconds(10)).max("score")
                ;
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
