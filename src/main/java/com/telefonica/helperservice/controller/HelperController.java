package com.telefonica.helperservice.controller;

import com.telefonica.helperservice.dto.DateConversionRequest;
import com.telefonica.helperservice.dto.DateConversionResponse;
import com.telefonica.helperservice.service.DateConversionService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.time.format.DateTimeParseException;
import java.util.Map;

@RestController
@RequestMapping("/api/helpers")
public class HelperController {

    private final DateConversionService dateConversionService;

    public HelperController(DateConversionService dateConversionService) {
        this.dateConversionService = dateConversionService;
    }

    @PostMapping("/date/german")
    public DateConversionResponse convertToGermanDate(@Valid @RequestBody DateConversionRequest request) {
        String germanDate = dateConversionService.toGermanDate(request.dateValue());
        return new DateConversionResponse(request.dateValue(), germanDate);
    }

    @ExceptionHandler(DateTimeParseException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> handleDateFormatError(DateTimeParseException ex) {
        return Map.of(
                "error", "Ungültiges Datumsformat",
                "detail", "Erlaubt sind z. B. 2026-02-09, 09.02.2026 oder ISO-Datetime"
        );
    }
}
