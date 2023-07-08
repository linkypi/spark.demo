package com.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @Desc
 * @Author linkypi
 * @Email trouble.linky@gmail.com
 * @Date 2023-07-08 15:07
 */
public class ClickSource implements SourceFunction<Event> {

    private long count = 1l;
    private boolean running = true;

    public void run(SourceContext<Event> sourceContext) throws Exception {

        Random random = new Random();
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=100", "./prod?id=10"};

        while (running) {
            String user = users[ random.nextInt(users.length)];
            String url = urls[ random.nextInt(urls.length)];
            long timeInMillis = Calendar.getInstance().getTimeInMillis();
            Event event = new Event(user, url, timeInMillis);
            sourceContext.collect(event);
            count++;
            Thread.sleep(1000);
        }
    }

    public void cancel() {
        running = false;
    }

}
