package com.flink;

import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author leo
 * @ClassName DataStream_JdbcSink_ExactlyOnce
 * @description:
 * 注意:
 * 1、JdbcSink.exactLyOnceSink() 该方法是恰好一次语义，也就是结果不重复。
 * 2、精确一次语义需要设置属性: .withMaxRetries(0)，否则，可能会产生重复的结果
 * 3、连接需要为支持XA事务的数据源连接: MysglXADataSource
 * 4、精确一次语义需要开启Checkpoint，否则结果无法刷新到数据库。因为JdbcXaSinkFunction实现了CheckpointFunction.
 * @date 7/1/23 1:08 PM
 */
public class DataStream_JdbcSink_ExactlyOnce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 开启 checkpoint
        environment.enableCheckpointing(1000);

        DataStream<String> dataStream = environment.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<Tuple2<Integer, String>> streamOperator = dataStream.map((record) -> {
            String[] fields = record.split(" ");
            return Tuple2.of(Integer.valueOf(fields[0]), fields[1]);
        }).returns(new TypeHint<Tuple2< Integer,String>>(){});
        streamOperator.addSink(JdbcSink.exactlyOnceSink(
                "insert into books(bookID, bookName) values (?,?)",
                (ps, book) -> {
                    ps.setInt(1, book.f0);
                    ps.setString(2, book.f1);
                },
                JdbcExecutionOptions.builder()
//                        .withBatchSize(1000) // 默认 5000
//                        .withBatchIntervalMs(500) // 默认 0
                        .withMaxRetries(0) //应设置为 0, 如果大于 0则容易导致数据重复
                        .build(),
                JdbcExactlyOnceOptions.builder()
                        .withTransactionPerConnection(true) // 每个连接开启一个事务
                        .build(),
                ()->{
                    MysqlXADataSource mysqlXADataSource = new MysqlXADataSource();
                    mysqlXADataSource.setURL("jdbc:mysql://localhost:3306/test?characterEncoding=UTF-8");
                    mysqlXADataSource.setUser("root");
                    mysqlXADataSource.setPassword("12345678");
                    return mysqlXADataSource;
                }
        ));
        environment.execute("jdbcExactlyOnceSink");
    }
}
