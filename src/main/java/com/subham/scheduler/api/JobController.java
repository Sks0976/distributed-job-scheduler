package com.subham.scheduler.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.subham.scheduler.domain.JobExecution;
import com.subham.scheduler.domain.JobStatus;
import com.subham.scheduler.service.JobService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobService service;
    private final ObjectMapper mapper;

    public JobController(JobService service, ObjectMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    /** 201 when created, 200 when an existing job with the same idempotency key is returned. */
    @PostMapping
    public ResponseEntity<JobResponse> create(@Valid @RequestBody CreateJobRequest request) {
        JobService.CreateResult result = service.create(request);
        HttpStatus status = result.created() ? HttpStatus.CREATED : HttpStatus.OK;
        return ResponseEntity.status(status).body(JobResponse.from(result.job(), mapper));
    }

    @PostMapping("/batch")
    public ResponseEntity<Map<String, Integer>> createBatch(@Valid @RequestBody BatchCreateRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("created", service.createBatch(request.jobs())));
    }

    @GetMapping("/{id}")
    public JobResponse get(@PathVariable UUID id) {
        return JobResponse.from(service.get(id), mapper);
    }

    @GetMapping
    public List<JobResponse> list(@RequestParam(required = false) JobStatus status,
                                  @RequestParam(defaultValue = "50") int limit) {
        return service.list(status, limit).stream().map(job -> JobResponse.from(job, mapper)).toList();
    }

    @GetMapping("/{id}/executions")
    public List<JobExecution> executions(@PathVariable UUID id) {
        return service.executions(id);
    }

    @PostMapping("/{id}/cancel")
    public JobResponse cancel(@PathVariable UUID id) {
        return JobResponse.from(service.cancel(id), mapper);
    }

    @PostMapping("/{id}/retry")
    public JobResponse retry(@PathVariable UUID id) {
        return JobResponse.from(service.retry(id), mapper);
    }

    @GetMapping("/stats")
    public Map<JobStatus, Long> stats() {
        return service.stats();
    }

    @GetMapping("/types")
    public Set<String> types() {
        return service.jobTypes();
    }
}
