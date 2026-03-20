package ru.quipy.common.utils

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Duration

@Configuration
class CircuitBreakerConfiguration  {

    @Bean
    fun paymentCircuitBreaker(): CircuitBreaker {
        val config = CircuitBreakerConfig.custom()
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
            .slidingWindowSize(50)
            .failureRateThreshold(10f)
            .waitDurationInOpenState(Duration.ofSeconds(5))
            .build()

        return CircuitBreaker.of("payment-cb", config)
    }
}