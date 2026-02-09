package com.telefonica.helperservice.dto;

import jakarta.validation.constraints.NotBlank;

public record DateConversionRequest(
        @NotBlank(message = "dateValue darf nicht leer sein")
        String dateValue
) {
}
