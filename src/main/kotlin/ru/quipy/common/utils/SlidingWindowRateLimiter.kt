package ru.quipy.common.utils

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

class SlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration,
) : RateLimiter {
    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    private val currentCount = AtomicInteger(0)
    private val timestamps = ConcurrentLinkedQueue<Long>()

    override fun tick(): Boolean {
        removeExpired()
        val now = System.currentTimeMillis()
        if (currentCount.incrementAndGet() <= rate) {
            timestamps.add(now)
            return true
        } else {
            currentCount.decrementAndGet()
            return false
        }
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
            removeExpired()
            if (currentCount.get() < rate) {
                val now = System.currentTimeMillis()
                if (currentCount.incrementAndGet() <= rate) {
                    timestamps.add(now)
                    return true
                } else {
                    currentCount.decrementAndGet()
                }
            }
            delay(10)
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