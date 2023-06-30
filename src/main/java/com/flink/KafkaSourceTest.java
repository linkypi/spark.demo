package com.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractPartitionDiscoverer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;
import org.apache.flink.util.SerializedValue;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * @author: lynch
 * @description:
 * @date: 2023/6/27 21:08
 */
public class KafkaSourceTest {

    public static String servers = "192.168.1.2:9093,192.168.1.2:9094,192.168.1.2:9095";

    public static void main(String[] args) throws Exception {
        consumerTop();
    }
    public static void consumerMessage(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.2:9093,192.168.1.2:9094,192.168.1.2:9095");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache,kafka.common,serialization,StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache,kafka.common,serialization,StringDeserializer");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("clicks",
                new SimpleStringSchema(), properties);
        DataStreamSource<String> streamSource = environment.addSource(kafkaConsumer);

        streamSource.print();
        environment.execute();
    }

    private final static String TOPIC_NAME = "clicks";
    private final static String CONSUMER_GROUP_NAME = "testGroup";


    private static void consumerTop() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        // 消费分组名
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_NAME);

        // 是否自动提交offset，默认就是true-不建议自动提交
        /*props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 自动提交offset的间隔时间
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");*/
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        /*
        当消费主题的是一个新的消费组，或者指定offset的消费方式，offset不存在，那么应该如何消费
        latest(默认) ：只消费自己启动之后发送到主题的消息
        earliest：第一次从头开始消费，以后按照消费offset记录继续消费，这个需要区别于consumer.seekToBeginning(每次都从头开始消费)
        */
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

		/*
		consumer给broker发送心跳的间隔时间，broker接收到心跳如果此时有rebalance发生会通过心跳响应将
		rebalance方案下发给consumer，这个时间可以稍微短一点
		*/
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1000);

        /*
        服务端broker多久感知不到一个consumer心跳就认为他故障了，会将其踢出消费组，
        对应的Partition也会被重新分配给其他consumer，默认是10秒
        */
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10 * 1000);

        //一次poll最大拉取消息的条数，如果消费者处理速度很快，可以设置大点，如果处理速度一般，可以设置小点
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
        /*
        如果两次poll操作间隔超过了这个时间，broker就会认为这个consumer处理能力太弱，
        会将其踢出消费组，将分区分配给别的consumer消费  ——优胜劣汰，也是埋坑点，可能会消费不到消息，每次都被踢
        */
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 30 * 1000);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
//            // 消费指定分区
//            consumer.assign(Collections.singletonList(new TopicPartition(TOPIC_NAME, 0)));
//
//            //消息回溯消费——是否从头开始消费
//            consumer.assign(Collections.singletonList(new TopicPartition(TOPIC_NAME, 0)));
//            consumer.seekToBeginning(Collections.singletonList(new TopicPartition(TOPIC_NAME, 0)));

            //指定offset消费
//            consumer.assign(Collections.singletonList(new TopicPartition(TOPIC_NAME, 0)));
//            consumer.seek(new TopicPartition(TOPIC_NAME, 0), 10);

//            //从指定时间点开始消费
//            List<PartitionInfo> topicPartitions = consumer.partitionsFor(TOPIC_NAME);
//            //从1小时前开始消费
//            long fetchDataTime = new Date().getTime() - 1000 * 60 * 60;
//            Map<TopicPartition, Long> map = new HashMap<>();
//            for (PartitionInfo par : topicPartitions) {
//                map.put(new TopicPartition(TOPIC_NAME, par.partition()), fetchDataTime);
//            }
//            Map<TopicPartition, OffsetAndTimestamp> parMap = consumer.offsetsForTimes(map);
//            for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : parMap.entrySet()) {
//                TopicPartition key = entry.getKey();
//                OffsetAndTimestamp value = entry.getValue();
//                if (key == null || value == null) continue;
//                Long offset = value.offset();
//                System.out.println("partition-" + key.partition() + "|offset-" + offset);
//                System.out.println();
//                //根据消费里的timestamp确定offset
//                consumer.assign(Arrays.asList(key));
//                consumer.seek(key, offset);
//            }


            while (true) {
                /*
                 * poll() API 是拉取消息的长轮询
                 */
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (records.count() > 0) {
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.printf("收到消息：partition = %d,offset = %d, key = %s, value = %s%n", record.partition(),
                                record.offset(), record.key(), record.value());
                    }
                    // 手动同步提交offset，当前线程会阻塞直到offset提交成功
                    // 一般使用同步提交，因为提交之后一般也没有什么逻辑代码了
                    //consumer.commitSync();

                    // 手动异步提交offset，当前线程提交offset不会阻塞，可以继续处理后面的程序逻辑
                /*consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        if (exception != null) {
                            System.err.println("Commit failed for " + offsets);
                            System.err.println("Commit failed exception: " + exception.getStackTrace());
                        }
                    }
                });*/
                } else {
                    System.out.println("no records");
                }
            }
        }
    }

    public static void produceMessages(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for(int i = 0; i < 1000; i++) {
            Thread.sleep(2000);
            System.out.println("produce "+ i);
            producer.send(new ProducerRecord<String, String>("clicks", Integer.toString(i), Integer.toString(i)));
        }
        producer.close();
    }

}
