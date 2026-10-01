package com.subham.scheduler.handler;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class JobHandlerRegistry {

    private final Map<String, JobHandler> handlers;

    public JobHandlerRegistry(List<JobHandler> handlers) {
        this.handlers = handlers.stream().collect(Collectors.toMap(JobHandler::type, Function.identity(), (a, b) -> {
            throw new IllegalStateException("Duplicate job handler for type '" + a.type() + "'");
        }));
    }

    public Optional<JobHandler> find(String type) {
        return Optional.ofNullable(handlers.get(type));
    }

    public Set<String> types() {
        return new TreeSet<>(handlers.keySet());
    }
}
