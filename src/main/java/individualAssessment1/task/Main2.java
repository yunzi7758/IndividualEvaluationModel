package individualAssessment1.task;

import com.alibaba.fastjson.JSON;
import individualAssessment1.domain.Event;
import individualAssessment1.map.CountScoreMap;
import individualAssessment1.uitl.KafkaUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Desc: socket
 * Created by zhisheng on 2019-04-26
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class Main2 {
    public static void main(String[] args) throws Exception {

//        oneState(args);
        moreState();
        return;
    }

    private static void moreState() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1 分钟一次CheckPoint
        env.enableCheckpointing(TimeUnit.MINUTES.toMillis(1));
        env.setParallelism(2);
//
//        CheckpointConfig checkpointConf = env.getCheckpointConfig();
//        // CheckPoint 语义 EXACTLY ONCE
//        checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties props = getProperties();


        System.out.println("=================start");

        DataStreamSource<String> appInfoSource = env.addSource(new FlinkKafkaConsumer011<>(
                // kafka topic， String 序列化
                KafkaUtil.TOPIC, new SimpleStringSchema(),
//                KafkaConfigUtil.buildKafkaProps( ExecutionEnvUtil.PARAMETER_TOOL))
                props
        ));


//        appInfoSource.print();

        // 按照 appId 进行 keyBy
        appInfoSource
                .keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String s) throws Exception {
                Event event = JSON.parseObject(s, Event.class);

                return (String) event.getPayload().get("member_id");
            }
        })
                .map(new CountScoreMap())
                .timeWindowAll(Time.seconds(10))
//                .setParallelism(1)
        ;


        // 定时调用sink，但是in数据还是逐条过来
//        apply.addSink(new SinkFunction<Tuple2<String, OutData>>() {
//            @Override
//            public void invoke(Tuple2<String, OutData> value, Context context) throws Exception {
////                System.out.println(value._2);
//
//                System.out.println(value._2.getFinalIaOutDatas());
//
//            }
//        });

        System.out.println("=================end");
        env.execute("Flink ia stat");
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
