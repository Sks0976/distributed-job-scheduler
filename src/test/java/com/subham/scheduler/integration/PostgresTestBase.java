package com.subham.scheduler.integration;

import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Runs tests against a real PostgreSQL in Docker, since SKIP LOCKED semantics cannot be faked.
 * One container is shared by all test classes (singleton pattern) and removed when the JVM exits.
 */
abstract class PostgresTestBase {

    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    static {
        POSTGRES.start();
    }
}
