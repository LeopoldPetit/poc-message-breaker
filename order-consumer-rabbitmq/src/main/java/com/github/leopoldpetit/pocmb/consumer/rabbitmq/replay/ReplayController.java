package com.github.leopoldpetit.pocmb.consumer.rabbitmq.replay;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/replay")
public class ReplayController {

    private final StreamReplayService replayService;

    public ReplayController(StreamReplayService replayService) {
        this.replayService = replayService;
    }

    /** POST /api/replay replays the stream from the first offset; add timestamp=epochMillis to start from a time. */
    @PostMapping
    public ReplayResult replay(@RequestParam(name = "timestamp", required = false) Long timestamp) {
        return replayService.replay(timestamp);
    }
}
