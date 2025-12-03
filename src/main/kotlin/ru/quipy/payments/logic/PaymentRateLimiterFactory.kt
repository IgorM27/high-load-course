package ru.quipy.payments.logic

import org.springframework.stereotype.Component
import ru.quipy.common.utils.NonBlockingSlidingWindowRateLimiter
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

@Component
class PaymentRateLimiterFactory {

    private val rateLimiters = ConcurrentHashMap<String, SlidingWindowRateLimiter>()
    private val nonBlockingRateLimiters = ConcurrentHashMap<String, NonBlockingSlidingWindowRateLimiter>()

    fun getRateLimiterForAccount(accountName: String, rateLimitPerSec: Int): SlidingWindowRateLimiter {
        return rateLimiters.computeIfAbsent(accountName) {
            SlidingWindowRateLimiter(
                rate = rateLimitPerSec.toLong(),
                window = Duration.ofSeconds(1)
            )
        }
    }

    fun getNonBlockingRateLimiter(accountName: String, rateLimitPerSec: Int): NonBlockingSlidingWindowRateLimiter {
        return nonBlockingRateLimiters.computeIfAbsent(accountName) {
            NonBlockingSlidingWindowRateLimiter(
                rate = rateLimitPerSec,
                windowMs = 1000L
            )
        }
    }
}
