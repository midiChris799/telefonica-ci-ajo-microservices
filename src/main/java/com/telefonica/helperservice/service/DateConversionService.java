package com.telefonica.helperservice.service;

import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

@Service
public class DateConversionService {

    private static final DateTimeFormatter GERMAN_OUTPUT_FORMATTER = DateTimeFormatter
            .ofPattern("dd.MM.yyyy", Locale.GERMAN);

    private final List<Function<String, LocalDate>> parseStrategies = List.of(
            this::parseIsoLocalDate,
            this::parseGermanInputDate,
            this::parseIsoLocalDateTime,
            this::parseIsoOffsetDateTime,
            this::parseIsoZonedDateTime,
            this::parseInstant
    );

    public String toGermanDate(String rawDateValue) {
        for (Function<String, LocalDate> parseStrategy : parseStrategies) {
            try {
                LocalDate parsedDate = parseStrategy.apply(rawDateValue);
                return parsedDate.format(GERMAN_OUTPUT_FORMATTER);
            } catch (DateTimeParseException ignored) {
                // nächstes Datumsformat probieren
            }
        }

        throw new DateTimeParseException("Unbekanntes Datumsformat", rawDateValue, 0);
    }

    private LocalDate parseIsoLocalDate(String value) {
        return LocalDate.parse(value, DateTimeFormatter.ISO_LOCAL_DATE);
    }

    private LocalDate parseGermanInputDate(String value) {
        DateTimeFormatter germanInputFormatter = DateTimeFormatter.ofPattern("d.M.uuuu", Locale.GERMAN);
        return LocalDate.parse(value, germanInputFormatter);
    }

    private LocalDate parseIsoLocalDateTime(String value) {
        return LocalDateTime.parse(value, DateTimeFormatter.ISO_LOCAL_DATE_TIME).toLocalDate();
    }

    private LocalDate parseIsoOffsetDateTime(String value) {
        return OffsetDateTime.parse(value, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toLocalDate();
    }

    private LocalDate parseIsoZonedDateTime(String value) {
        return ZonedDateTime.parse(value, DateTimeFormatter.ISO_ZONED_DATE_TIME).toLocalDate();
    }

    private LocalDate parseInstant(String value) {
        return Instant.parse(value).atZone(java.time.ZoneOffset.UTC).toLocalDate();
    }
}
