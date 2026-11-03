package com.github.leopoldpetit.pocmb.producer.load;

import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/load")
public class LoadController {

    private final LoadGenerator generator;

    public LoadController(LoadGenerator generator) {
        this.generator = generator;
    }

    @PostMapping("/start")
    public LoadStatus start(@RequestBody(required = false) LoadRequest request) {
        return generator.start(request == null ? new LoadRequest(null, null, null, null, null, null, null, null, null) : request);
    }

    @PostMapping("/stop")
    public LoadStatus stop() {
        return generator.stop();
    }

    @GetMapping("/status")
    public LoadStatus status() {
        return generator.status();
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
    }
}
