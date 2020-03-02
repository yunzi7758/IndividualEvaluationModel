package com.zhisheng.examples.util;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class KafkaUtil {


    public static final String broker_list = "localhost:9092";
    private static final HashMap<String, Long> producerMap = new HashMap<>();



    private static final KafkaProducer producer;
    static {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
         producer = new KafkaProducer<String, String>(props);
    }
    public static void sendToKafka(String topic , String msg){

        ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, msg);
        producer.send(record);
        System.out.println("发送数据: " + msg);
        producer.flush();
    }
    public static void main(String[] args) throws InterruptedException {
//        comsume();
//        produce();
        produceOnce();
        return;
    }

    private static void produceOnce() {
        sendToKafka(KafkaUtil.TOPIC,"{\"table\":\"gree\",\"payload\":{\"date\":\"2020-01-15\",\"tenant_id\":\"gree\",\"member_id\":\"15521177989111\",\"sourceId\":330,\"created_time\":1579086719050,\"ip\":\"120.198.22.24\",\"channel\":\"weixin\",\"type\":\"login\",\"platform\":\"Unknown\",\"stay\":0,\"account_id\":\"15521177989\",\"system\":\"Unknown\",\"commodity_id\":\"\",\"money\":0,\"engine\":\"Unknown\",\"activity_id\":\"\",\"active_marking_id\":\"\",\"device\":\"PC\",\"order_id\":\"\"}}");
        sendToKafka(KafkaUtil.TOPIC,"{\"table\":\"gree\",\"payload\":{\"date\":\"2020-01-15\",\"tenant_id\":\"gree\",\"member_id\":\"15521177989111\",\"sourceId\":330,\"created_time\":1579086719050,\"ip\":\"120.198.22.24\",\"channel\":\"weixin\",\"type\":\"pay\",\"platform\":\"Unknown\",\"stay\":0,\"account_id\":\"15521177989\",\"system\":\"Unknown\",\"commodity_id\":\"\",\"money\":0,\"engine\":\"Unknown\",\"activity_id\":\"\",\"active_marking_id\":\"\",\"device\":\"PC\",\"order_id\":\"\"}}");
        sendToKafka(KafkaUtil.TOPIC,"{\"table\":\"gree\",\"payload\":{\"date\":\"2020-01-15\",\"tenant_id\":\"gree\",\"member_id\":\"15521177989110\",\"sourceId\":330,\"created_time\":1579086719050,\"ip\":\"120.198.22.24\",\"channel\":\"weixin\",\"type\":\"login\",\"platform\":\"Unknown\",\"stay\":0,\"account_id\":\"15521177989\",\"system\":\"Unknown\",\"commodity_id\":\"\",\"money\":0,\"engine\":\"Unknown\",\"activity_id\":\"\",\"active_marking_id\":\"\",\"device\":\"PC\",\"order_id\":\"\"}}");
        sendToKafka(KafkaUtil.TOPIC,"{\"table\":\"gree\",\"payload\":{\"date\":\"2020-01-15\",\"tenant_id\":\"gree\",\"member_id\":\"15521177989110\",\"sourceId\":330,\"created_time\":1579086719050,\"ip\":\"120.198.22.24\",\"channel\":\"weixin\",\"type\":\"pay\",\"platform\":\"Unknown\",\"stay\":0,\"account_id\":\"15521177989\",\"system\":\"Unknown\",\"commodity_id\":\"\",\"money\":0,\"engine\":\"Unknown\",\"activity_id\":\"\",\"active_marking_id\":\"\",\"device\":\"PC\",\"order_id\":\"\"}}");

    }

    private static void produce() throws InterruptedException {
        while (true) {

            Thread.sleep(1000);
            sendToKafka(KafkaUtil.TOPIC,"{\"table\":\"gree\",\"payload\":{\"date\":\"2020-01-15\",\"tenant_id\":\"gree\",\"member_id\":\"15521177989111\",\"sourceId\":330,\"created_time\":1579086719050,\"ip\":\"120.198.22.24\",\"channel\":\"weixin\",\"type\":\"login\",\"platform\":\"Unknown\",\"stay\":0,\"account_id\":\"15521177989\",\"system\":\"Unknown\",\"commodity_id\":\"\",\"money\":0,\"engine\":\"Unknown\",\"activity_id\":\"\",\"active_marking_id\":\"\",\"device\":\"PC\",\"order_id\":\"\"}}");
            sendToKafka(KafkaUtil.TOPIC,"{\"table\":\"gree\",\"payload\":{\"date\":\"2020-01-15\",\"tenant_id\":\"gree\",\"member_id\":\"15521177989111\",\"sourceId\":330,\"created_time\":1579086719050,\"ip\":\"120.198.22.24\",\"channel\":\"weixin\",\"type\":\"pay\",\"platform\":\"Unknown\",\"stay\":0,\"account_id\":\"15521177989\",\"system\":\"Unknown\",\"commodity_id\":\"\",\"money\":0,\"engine\":\"Unknown\",\"activity_id\":\"\",\"active_marking_id\":\"\",\"device\":\"PC\",\"order_id\":\"\"}}");
            sendToKafka(KafkaUtil.TOPIC,"{\"table\":\"gree\",\"payload\":{\"date\":\"2020-01-15\",\"tenant_id\":\"gree\",\"member_id\":\"15521177989110\",\"sourceId\":330,\"created_time\":1579086719050,\"ip\":\"120.198.22.24\",\"channel\":\"weixin\",\"type\":\"login\",\"platform\":\"Unknown\",\"stay\":0,\"account_id\":\"15521177989\",\"system\":\"Unknown\",\"commodity_id\":\"\",\"money\":0,\"engine\":\"Unknown\",\"activity_id\":\"\",\"active_marking_id\":\"\",\"device\":\"PC\",\"order_id\":\"\"}}");
            sendToKafka(KafkaUtil.TOPIC,"{\"table\":\"gree\",\"payload\":{\"date\":\"2020-01-15\",\"tenant_id\":\"gree\",\"member_id\":\"15521177989110\",\"sourceId\":330,\"created_time\":1579086719050,\"ip\":\"120.198.22.24\",\"channel\":\"weixin\",\"type\":\"pay\",\"platform\":\"Unknown\",\"stay\":0,\"account_id\":\"15521177989\",\"system\":\"Unknown\",\"commodity_id\":\"\",\"money\":0,\"engine\":\"Unknown\",\"activity_id\":\"\",\"active_marking_id\":\"\",\"device\":\"PC\",\"order_id\":\"\"}}");

        }
    }

    private static void comsume() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                consume();
            }
        }).start();
    }


    public static String TOPIC = "ia1";

    private static final ConsumerConnector consumer;

    static  {
        Properties props = new Properties();
        //zookeeper 配置
        props.put("zookeeper.connect", "127.0.0.1:2181");

        //group 代表一个消费组
        props.put("group.id", "jd-group");

        //zk连接超时
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
        //序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        ConsumerConfig config = new ConsumerConfig(props);

        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
    }

    public static void consume() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(KafkaUtil.TOPIC, new Integer(1));

        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

        Map<String, List<KafkaStream<String, String>>> consumerMap =
                consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);
        KafkaStream<String, String> stream = consumerMap.get(KafkaUtil.TOPIC).get(0);
        ConsumerIterator<String, String> it = stream.iterator();
        while (it.hasNext()) {
            System.out.println("consume::::"+it.next().message());
        }
    }
}
