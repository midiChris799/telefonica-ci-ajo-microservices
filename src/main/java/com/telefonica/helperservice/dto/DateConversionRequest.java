package com.telefonica.helperservice.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;

public record DateConversionRequest(
        @Schema(description = "Beliebiges Datumsformat (z. B. 2026-02-09 oder 09.02.2026)", example = "2026-02-09")
        @NotBlank(message = "dateValue darf nicht leer sein")
        String dateValue
) {
}
