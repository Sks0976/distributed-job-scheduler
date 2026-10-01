package com.subham.scheduler.handler;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.UUID;

public record JobContext(UUID jobId, int attempt, JsonNode payload) {
}
