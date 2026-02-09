package com.telefonica.helperservice.dto;

import io.swagger.v3.oas.annotations.media.Schema;

public record DateConversionResponse(
        @Schema(example = "2026-02-09")
        String originalValue,
        @Schema(example = "09.02.2026")
        String germanDate
) {
}
