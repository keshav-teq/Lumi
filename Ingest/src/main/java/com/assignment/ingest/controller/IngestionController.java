package com.assignment.ingest.controller;

import com.assignment.ingest.dto.IngestionRequest;
import com.assignment.ingest.dto.IngestionResponse;
import com.assignment.ingest.service.IngestionService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * IngestionController defines the POST /api/ingestions mapping
 * to trigger the DAG with the IngestionRequest body.
 */
@RestController
@RequestMapping("/api/ingestions")
public class IngestionController {

    private final IngestionService service;

    public IngestionController(IngestionService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<IngestionResponse> trigger(@Valid @RequestBody IngestionRequest req) {
        // 202 = accepted for async processing (the DAG runs in the background).
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(service.startIngestion(req));
    }
}