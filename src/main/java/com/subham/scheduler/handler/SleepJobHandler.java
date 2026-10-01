package com.subham.scheduler.handler;

import org.springframework.stereotype.Component;

/** Simulates work of a given duration. Payload: {"millis": 200} */
@Component
public class SleepJobHandler implements JobHandler {

    @Override
    public String type() {
        return "sleep";
    }

    @Override
    public void execute(JobContext context) throws InterruptedException {
        long millis = context.payload().path("millis").asLong(100);
        if (millis < 0) {
            throw new NonRetryableJobException("millis must be >= 0");
        }
        Thread.sleep(millis);
    }
}
