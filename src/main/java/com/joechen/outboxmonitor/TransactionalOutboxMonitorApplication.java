package com.joechen.outboxmonitor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.retry.annotation.EnableRetry;

@EnableRetry
@SpringBootApplication
public class TransactionalOutboxMonitorApplication {

    public static void main(String[] args) {
        SpringApplication.run(TransactionalOutboxMonitorApplication.class, args);
    }
}
