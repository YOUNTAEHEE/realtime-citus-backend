package com.yth.realtime.config;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ThreadPoolConfig {
    @Bean(name = "modbusThreadPool")
    public ExecutorService modbusThreadPool() {
        // 모드버스 전용 스레드 풀 구성 (우선순위 높게)
        ThreadFactory factory = new ThreadFactory() {
            private final AtomicInteger count = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("modbus-worker-" + count.incrementAndGet());
                thread.setPriority(Thread.MAX_PRIORITY); // 최대 우선순위 설정
                return thread;
            }
        };

        return Executors.newFixedThreadPool(3, factory);
    }

    @Bean(name = "opcuaThreadPool")
    public ExecutorService opcuaThreadPool() {
        // OPC UA 전용 스레드 풀 구성 (보통 우선순위)
        ThreadFactory factory = new ThreadFactory() {
            private final AtomicInteger count = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("opcua-worker-" + count.incrementAndGet());
                thread.setPriority(Thread.NORM_PRIORITY); // 보통 우선순위 설정
                return thread;
            }
        };

        return Executors.newFixedThreadPool(3, factory);
    }
}