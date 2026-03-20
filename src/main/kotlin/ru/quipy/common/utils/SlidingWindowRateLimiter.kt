package ru.quipy.common.utils

import kotlinx.coroutines.delay
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

class SlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration,
) : RateLimiter {

    private val currentCount = AtomicInteger(0)
    private val timestamps = ConcurrentLinkedQueue<Long>()

    override fun tick(): Boolean {
        removeExpired()
        val now = System.currentTimeMillis()
        val count = currentCount.incrementAndGet()
        return if (count <= rate) {
            timestamps.add(now)
            true
        } else {
            currentCount.decrementAndGet()
            false
        }
    }

    fun tickBlocking(timeout: Duration): Boolean {
        val end = System.currentTimeMillis() + timeout.toMillis()
        while (System.currentTimeMillis() <= end) {
            if (tick()) return true
            Thread.sleep(10)
        }

        return false
    }

    suspend fun tickSuspend(timeout: Duration) : Boolean {
        val end = System.currentTimeMillis() + timeout.toMillis()
        while (System.currentTimeMillis() <= end) {
            if (tick()) return true
            delay(10)
        }

        return false
    }

    fun tickBlocking() {
        while (!tick()) {
            Thread.sleep(10)
        }
    }

    suspend fun tickSuspending() {
        while (!tick()) {
            delay(10)
        }
    }

    suspend fun acquireSuspend(timeoutMillis: Long): Boolean {
        val deadline = System.currentTimeMillis() + timeoutMillis
        while (System.currentTimeMillis() < deadline) {
            if (tick()) return true
            val remaining = deadline - System.currentTimeMillis()
            if (remaining <= 0) break
            delay(minOf(10L, remaining))
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

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(SlidingWindowRateLimiter::class.java)
    }
}