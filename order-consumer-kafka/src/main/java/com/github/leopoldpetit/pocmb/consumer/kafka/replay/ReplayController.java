package com.github.leopoldpetit.pocmb.consumer.kafka.replay;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/replay")
public class ReplayController {

    private final KafkaReplayService replayService;

    public ReplayController(KafkaReplayService replayService) {
        this.replayService = replayService;
    }

    /** POST /api/replay replays from the earliest offset; add timestamp=epochMillis to start from a time. */
    @PostMapping
    public ReplayResult replay(@RequestParam(name = "timestamp", required = false) Long timestamp) {
        return replayService.replay(timestamp);
    }
}
