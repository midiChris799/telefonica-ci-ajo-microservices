# Helper Service (Spring Boot)

Dieser Microservice stellt Helper-Endpunkte bereit. Aktuell ist ein Helper für Datumsumwandlungen enthalten.

## Endpunkt

- `POST /api/helpers/date/german`

### Request

```json
{
  "dateValue": "2026-02-09T15:30:00+01:00"
}
```

### Response

```json
{
  "originalValue": "2026-02-09T15:30:00+01:00",
  "germanDate": "09.02.2026"
}
```

## Swagger / OpenAPI

Nach dem Start der Anwendung sind folgende URLs verfügbar:

- Swagger UI: `http://localhost:8080/swagger-ui.html`
- OpenAPI JSON: `http://localhost:8080/api-docs`

Damit kannst du die Endpunkte direkt im Browser testen.

## Unterstützte Eingabeformate

- `yyyy-MM-dd`
- `d.M.yyyy`
- ISO Local DateTime (`yyyy-MM-ddTHH:mm:ss`)
- ISO Offset DateTime (`yyyy-MM-ddTHH:mm:ss+01:00`)
- ISO Zoned DateTime (`yyyy-MM-ddTHH:mm:ss+01:00[Europe/Berlin]`)
- ISO Instant (`yyyy-MM-ddTHH:mm:ssZ`)

## Starten

```bash
mvn spring-boot:run
```

## Testen

```bash
mvn test
```
