package com.joechen.outboxmonitor.consumer;

import jakarta.validation.constraints.NotBlank;

public record ReplayRequest(
        @NotBlank String reason
) {
}
