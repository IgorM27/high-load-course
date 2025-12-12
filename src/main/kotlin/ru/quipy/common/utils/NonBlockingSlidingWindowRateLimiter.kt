package ru.quipy.common.utils

import kotlinx.coroutines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicLong


class NonBlockingSlidingWindowRateLimiter(
    private val rate: Int,
    private val windowMs: Long = 1000L
) {
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(NonBlockingSlidingWindowRateLimiter::class.java)
    }

    private val counter = AtomicLong(0)
    private val timestamps = java.util.concurrent.ConcurrentLinkedDeque<Long>()

    private val scope = CoroutineScope(
        Dispatchers.Default + SupervisorJob() + CoroutineName("NonBlockingSlidingWindowRateLimiter")
    )

    init {
        scope.launch {
            while (true) {
                delay(windowMs / 4)
                cleanupExpired()
            }
        }
    }

    private fun cleanupExpired() {
        val threshold = System.currentTimeMillis() - windowMs
        while (true) {
            val head = timestamps.peekFirst() ?: break
            if (head < threshold) {
                timestamps.pollFirst()
                counter.decrementAndGet()
            } else {
                break
            }
        }
    }

    fun tryAcquire(): Boolean {
        cleanupExpired()

        while (true) {
            val current = counter.get()
            if (current >= rate) {
                return false
            }
            if (counter.compareAndSet(current, current + 1)) {
                timestamps.addLast(System.currentTimeMillis())
                return true
            }
        }
    }

    suspend fun acquireSuspendWithTimeout(timeoutMs: Long): Boolean {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (!tryAcquire()) {
            if (System.currentTimeMillis() >= deadline) {
                return false
            }
            delay(1)
        }
        return true
    }
}

