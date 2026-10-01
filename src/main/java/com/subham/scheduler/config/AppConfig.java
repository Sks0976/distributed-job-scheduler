package com.subham.scheduler.config;

import com.subham.scheduler.worker.BackoffPolicy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration
public class AppConfig {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    public BackoffPolicy backoffPolicy(SchedulerProperties properties) {
        SchedulerProperties.Retry retry = properties.retry();
        return new BackoffPolicy(retry.baseDelay(), retry.maxDelay(), retry.jitter());
    }
}
