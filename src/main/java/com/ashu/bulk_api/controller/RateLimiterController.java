package com.ashu.bulk_api.controller;

import com.google.common.util.concurrent.RateLimiter;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/config")
public class RateLimiterController {

    private final RateLimiter rateLimiter;

    public RateLimiterController(RateLimiter rateLimiter) {
        this.rateLimiter = rateLimiter;
    }

    @PostMapping("/rate")
    public String updateRate(@RequestParam double permitsPerSecond) {
        if (permitsPerSecond <= 0) return "Rate must be > 0";

        rateLimiter.setRate(permitsPerSecond);
        return "RateLimiter updated to " + permitsPerSecond + " requests/sec";
    }

    @GetMapping("/rate")
    public String getRate() {
        return "Current rate: " + rateLimiter.getRate() + " requests/sec";
    }
}
