package com.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @author leo
 * @ClassName DataStream_KafkaSink
 * @description: TODO
 * @date 7/1/23 2:33 PM
 */
public class DataStream_KafkaSink {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = environment.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<String> streamOperator = streamSource.map((r) -> {
            return r.split(" ")[1];
        });
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.2:9093,192.168.1.2:9094,192.168.1.2:9095");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 方式 1
//        KafkaRecordSerializationSchema<String> serializationSchema =
//                KafkaRecordSerializationSchema.<String>builder()
//                .setTopic("test")
//                .setValueSerializationSchema(new SimpleStringSchema())
//                .build();
//        KafkaSink<String> test = KafkaSink.<String>builder().setKafkaProducerConfig(props)
//                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
//                .setRecordSerializer(serializationSchema).build();
//        streamOperator.sinkTo(test);

        // 方式 2 已经废弃
        streamSource.addSink(new FlinkKafkaProducer<String>("test", new SimpleStringSchema(), props)).setParallelism(2);
    }
}
