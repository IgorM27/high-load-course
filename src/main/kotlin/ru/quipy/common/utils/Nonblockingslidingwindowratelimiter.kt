package ru.quipy.common.utils

import kotlinx.coroutines.*
import java.util.concurrent.atomic.AtomicLong
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

class NonBlockingSlidingWindowRateLimiter(
    private val rate: Int,
    private val window: Duration = Duration.ofSeconds(1)
) {
    private val timestamps = ConcurrentLinkedQueue<Long>()
    private val currentCount = AtomicInteger(0)

    suspend fun acquireSuspend(timeoutMillis: Long): Boolean {
        val deadline = System.currentTimeMillis() + timeoutMillis

        while (System.currentTimeMillis() < deadline) {
            removeExpired()
            if (currentCount.get() < rate) {
                val now = System.currentTimeMillis()
                if (tryAdd(now)) {
                    return true
                }
            }
            delay(1)
        }
        return false
    }

    private fun removeExpired() {
        val expiration = System.currentTimeMillis() - window.toMillis()
        while (true) {
            val ts = timestamps.peek() ?: break
            if (ts < expiration) {
                timestamps.poll()
                currentCount.decrementAndGet()
            } else {
                break
            }
        }
    }

    private fun tryAdd(now: Long): Boolean {
        if (currentCount.incrementAndGet() <= rate) {
            timestamps.add(now)
            return true
        } else {
            currentCount.decrementAndGet()
            return false
        }
    }
}