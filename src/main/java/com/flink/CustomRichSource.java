package com.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * @Desc
 * @Author linkypi
 * @Email trouble.linky@gmail.com
 * @Date 2019-08-21 17:52
 */
public class CustomRichSource extends RichParallelSourceFunction<Long> {

    private long count = 1l;
    private boolean running = true;

    public void run(SourceContext<Long> sourceContext) throws Exception {
        while (running) {
            sourceContext.collect(count);
            count++;
            Thread.sleep(500);
        }
    }

    public void cancel() {
        running = false;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("open....");
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        System.out.println("close...");
        super.close();
    }
}
