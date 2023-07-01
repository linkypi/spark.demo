package com.flink;


import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author leo
 * @ClassName DataStream_JDBC_Sink
 * @description:
 * JDBC sink指通过idbc方式将结果数据存储到支持JDBC的指定数据库中，但不是所有jdbc数据库都可以
 * 输入:
 *  # nc -lk 6666
 * 1 bigdqtabook
 * 2 javabook
 * 3 h5book
 * 输出结果到如下表:
 *  CREATE TABLE `books` (
 *    `bookID` int DEFAULT NULL,
 *    `bookName` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL
 *  ) ENGINE=InnODB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
 *
 * 注意:
 * 1、JdbcSink.sin()该方法是无恰好一次语义，也就是有可能会重复结果
 *
 * @date 7/1/23 11:08 AM
 */
public class DataStream_JdbcSink_AtLeastOnce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = environment.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<Tuple2<Integer, String>> streamOperator = dataStream.map((record) -> {
            String[] fields = record.split(" ");
            return Tuple2.of(Integer.valueOf(fields[0]), fields[1]);
        }).returns(new TypeHint<Tuple2< Integer,String>>(){});
        streamOperator.addSink(JdbcSink.sink(
                "insert into books(bookID, bookName) values (?,?)",
                (ps, book) -> {
                    ps.setInt(1, book.f0);
                    ps.setString(2, book.f1);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(500)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/test?characterEncoding=UTF-8")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("12345678").withConnectionCheckTimeoutSeconds(10).build()
        ));
        environment.execute("jdbcSink");
    }
}
