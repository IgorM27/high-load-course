package ru.quipy.common.utils

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.resume

class SuspendableRateLimiter(
    private val rate: Long,
    private val window: Duration,
) {
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(SuspendableRateLimiter::class.java)
    }

    private val windowMs = window.toMillis()
    private val tokens = AtomicLong(0)

    private val waitingQueue = Channel<CancellableContinuation<Unit>>(Channel.UNLIMITED)

    private val timestamps = java.util.concurrent.ConcurrentLinkedQueue<Long>()
    
    private val scope = CoroutineScope(
        Dispatchers.Default + SupervisorJob() + CoroutineName("SuspendableRateLimiter")
    )
    
    init {
        scope.launch {
            processWaitingQueue()
        }

        scope.launch {
            cleanupExpiredTimestamps()
        }
    }

    fun tryAcquire(): Boolean {
        cleanupOldTimestamps()
        
        while (true) {
            val current = tokens.get()
            if (current >= rate) {
                return false
            }
            if (tokens.compareAndSet(current, current + 1)) {
                timestamps.add(System.currentTimeMillis())
                return true
            }
        }
    }

    suspend fun acquire() {
        if (tryAcquire()) {
            return
        }

        suspendCancellableCoroutine { continuation ->
            waitingQueue.trySend(continuation)
        }
    }

    suspend fun acquireWithTimeout(timeout: Duration): Boolean {
        if (tryAcquire()) {
            return true
        }
        
        return withTimeoutOrNull(timeout.toMillis()) {
            suspendCancellableCoroutine<Unit> { continuation ->
                waitingQueue.trySend(continuation)
            }
            true
        } ?: false
    }
    
    private fun cleanupOldTimestamps() {
        val threshold = System.currentTimeMillis() - windowMs
        while (true) {
            val head = timestamps.peek() ?: break
            if (head <= threshold) {
                timestamps.poll()
                tokens.decrementAndGet()
            } else {
                break
            }
        }
    }
    
    private suspend fun processWaitingQueue() {
        while (true) {
            val continuation = waitingQueue.tryReceive().getOrNull()
            if (continuation != null) {
                while (!tryAcquire()) {
                    delay(1)
                }
                if (continuation.isActive) {
                    continuation.resume(Unit)
                }
            } else {
                delay(1)
            }
        }
    }
    
    private suspend fun cleanupExpiredTimestamps() {
        while (true) {
            delay(windowMs / 10)
            cleanupOldTimestamps()
        }
    }
    
    fun shutdown() {
        scope.cancel()
    }
}

class NonBlockingSlidingWindowRateLimiter(
    private val rate: Int,
    private val windowMs: Long = 1000L
) {
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(NonBlockingSlidingWindowRateLimiter::class.java)
    }

    private val counter = AtomicLong(0)
    private val timestamps = java.util.concurrent.ConcurrentLinkedDeque<Long>()
    private val mutex = Mutex()
    
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

    suspend fun acquireSuspend() {
        while (!tryAcquire()) {
            delay(1)
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
    
    fun shutdown() {
        scope.cancel()
    }
}

