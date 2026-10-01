package com.subham.scheduler.handler;

/**
 * Implement this and register it as a Spring bean to add a new job type.
 * Handlers should be idempotent: a job can run more than once (retries, or a lease that
 * expired while the original run was still going), so repeated execution must be safe.
 * Long-running handlers should check {@link Thread#interrupted()} so timeouts can stop them.
 */
public interface JobHandler {

    String type();

    void execute(JobContext context) throws Exception;
}
