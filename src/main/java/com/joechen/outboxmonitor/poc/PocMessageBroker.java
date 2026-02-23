package com.joechen.outboxmonitor.poc;

import org.springframework.stereotype.Component;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Component
public class PocMessageBroker {

    private final BlockingQueue<PocMessage> queue = new LinkedBlockingQueue<>();

    public void publish(PocMessage message) {
        queue.offer(message);
    }

    public PocMessage poll() {
        return queue.poll();
    }

    public int size() {
        return queue.size();
    }
}
