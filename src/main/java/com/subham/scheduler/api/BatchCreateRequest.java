package com.subham.scheduler.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;

import java.util.List;

public record BatchCreateRequest(@NotEmpty @Size(max = 1000) List<@Valid CreateJobRequest> jobs) {
}
