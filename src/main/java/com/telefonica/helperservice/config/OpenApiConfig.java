package com.telefonica.helperservice.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI helperServiceOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("Helper Service API")
                        .description("API zum Testen von Helper-Endpunkten (z. B. Datumsumwandlung)")
                        .version("v1")
                        .contact(new Contact().name("Helper Service Team")));
    }
}
