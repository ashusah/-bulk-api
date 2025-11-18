package com.ashu.bulk_api.config;

import com.google.common.util.concurrent.RateLimiter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor; // <-- This is correct

@Configuration
@EnableAsync
public class AsyncConfiguration {

    // Define RateLimiter: e.g., 25 requests per second
    private final RateLimiter rateLimiter = RateLimiter.create(25.0);

    @Bean(name = "bulkApiTaskExecutor")
    public TaskExecutor bulkApiTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(20);
        executor.setMaxPoolSize(50);
        executor.setQueueCapacity(10000);
        executor.setThreadNamePrefix("BulkAPI-");

        // Use ThreadPoolExecutor.CallerRunsPolicy directly
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

        executor.initialize();
        return executor;
    }
/*
    @Bean
    public Executor rateLimitedExecutor(TaskExecutor bulkApiTaskExecutor) {
        // Currently, semaphore-based rate limiting is implemented in service
        return bulkApiTaskExecutor;
    }
 */

    // Global rate limiter (requests per second)
    @Bean
    public RateLimiter rateLimiter() {
        return RateLimiter.create(100); // default 100 permits/sec
    }

    // Executor exposed for service (tasks are rate-limited)
    @Bean
    public Executor rateLimitedExecutor(TaskExecutor bulkApiTaskExecutor, RateLimiter rateLimiter) {
        return command -> bulkApiTaskExecutor.execute(() -> {
            rateLimiter.acquire(); // acquire permit before executing
            command.run();
        });
    }
}
