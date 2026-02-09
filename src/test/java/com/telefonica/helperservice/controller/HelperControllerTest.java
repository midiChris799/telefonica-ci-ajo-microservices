package com.telefonica.helperservice.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class HelperControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void shouldConvertIsoDateToGermanDate() throws Exception {
        mockMvc.perform(post("/api/helpers/date/german")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"dateValue\":\"2026-02-09\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.germanDate").value("09.02.2026"));
    }

    @Test
    void shouldConvertIsoDateTimeToGermanDate() throws Exception {
        mockMvc.perform(post("/api/helpers/date/german")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"dateValue\":\"2026-02-09T15:30:00+01:00\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.germanDate").value("09.02.2026"));
    }

    @Test
    void shouldReturnBadRequestForUnknownDateFormat() throws Exception {
        mockMvc.perform(post("/api/helpers/date/german")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"dateValue\":\"02/09/2026\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Ungültiges Datumsformat"));
    }
}
