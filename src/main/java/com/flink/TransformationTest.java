package com.flink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;

/**
 * @author: lynch
 * @description:
 * @date: 2023/6/27 20:40
 */
public class TransformationTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.fromElements();
//        DataStreamSource<Event> stream = env.fromElements (
//                new Event(  "Mary",  "./home" , 100L),
//        new Event(  "Bob" ,"./cart",  2000L),
//        new Event(  "Alice", "./prod?id=100",  3000L),
//        new Event( "Bob","./prod?id=1",  3300L),
//        new Event(  "Alice",  "./prod?id=200",  3200L),
//        new Event(  "Bob", "./home", 350L),
//        new Event( "Bob" , "./prod?id=2", 3800L),
//        new Event(  "Bob",  "./prod?id=3", 4200L));
    }
}
