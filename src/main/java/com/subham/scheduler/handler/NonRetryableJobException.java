package com.subham.scheduler.handler;

/** Thrown when retrying cannot help (bad payload, unknown type). The job fails immediately. */
public class NonRetryableJobException extends RuntimeException {

    public NonRetryableJobException(String message) {
        super(message);
    }
}
