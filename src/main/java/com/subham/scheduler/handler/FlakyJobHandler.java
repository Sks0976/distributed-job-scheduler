package com.subham.scheduler.handler;

import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Fails randomly to demonstrate retries and backoff.
 * Payload: {"failureRate": 0.5, "millis": 50}
 */
@Component
public class FlakyJobHandler implements JobHandler {

    @Override
    public String type() {
        return "flaky";
    }

    @Override
    public void execute(JobContext context) throws InterruptedException {
        double failureRate = context.payload().path("failureRate").asDouble(0.5);
        Thread.sleep(context.payload().path("millis").asLong(50));
        if (ThreadLocalRandom.current().nextDouble() < failureRate) {
            throw new IllegalStateException("Simulated transient failure on attempt " + context.attempt());
        }
    }
}
