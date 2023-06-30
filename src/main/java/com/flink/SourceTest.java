package com.flink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author: lynch
 * @description:
 * @date: 2023/6/27 20:48
 */
public class SourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件中读取
        DataStreamSource<String> streamSource = env.readTextFile("input/clicks.txt");
        streamSource.print();

        // 从集合读取
        List<Integer> list = new ArrayList<>();
        list.add(12);
        list.add(34);
        list.add(65);
        DataStreamSource<Integer> integerDataStreamSource = env.fromCollection(list);
        integerDataStreamSource.print();

        List<Event> events = new ArrayList<>();
        events.add(new Event("mary","/home",1000L));
        events.add(new Event("leo","/cat",3000L));
        events.add(new Event("ben","/home",4000L));
        DataStreamSource<Event> eventDataStreamSource = env.fromCollection(events);

        eventDataStreamSource.print();

        DataStreamSource<Event> fromElements = env.fromElements(new Event("mary", "/home", 1000L),
                new Event("leo", "/cat", 3000L));
        fromElements.print();

        // 从socket读取
        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9999);
        socketTextStream.print();

        env.execute();
    }
}
