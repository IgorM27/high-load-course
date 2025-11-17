package ru.quipy.common.utils

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.coroutines.resume

class SlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration,
) : RateLimiter {
    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    private val sum = AtomicLong(0)
    private val queue = PriorityBlockingQueue<Measure>(10_000)

    private val pendingContinuations = PriorityBlockingQueue<ContinuationWrapper>()
    private val lock = ReentrantLock()
    private val condition = lock.newCondition()

    override fun tick(): Boolean {
        while (true) {
            val curSum = sum.get()
            if (curSum >= rate) return false
            if (sum.compareAndSet(curSum, curSum + 1)) {
                queue.add(Measure(1, System.currentTimeMillis()))

                lock.withLock {
                    if (pendingContinuations.isNotEmpty()) {
                        condition.signal()
                    }
                }
                return true
            }
        }
    }

    fun tickBlocking() {
        while (!tick()) {
            Thread.sleep(10)
        }
    }

    suspend fun tickSuspend() {
        if (tick()) {
            return
        }

        return suspendCancellableCoroutine { continuation ->
            lock.withLock {
                if (tick()) {
                    continuation.resume(Unit)
                    return@withLock
                }

                val wrapper = ContinuationWrapper(continuation, System.currentTimeMillis())
                pendingContinuations.add(wrapper)

                continuation.invokeOnCancellation {
                    lock.withLock {
                        pendingContinuations.remove(wrapper)
                    }
                }
            }
        }
    }

    data class Measure(
        val value: Long,
        val timestamp: Long
    ) : Comparable<Measure> {
        override fun compareTo(other: Measure): Int {
            return timestamp.compareTo(other.timestamp)
        }
    }

    data class ContinuationWrapper(
        val continuation: kotlin.coroutines.Continuation<Unit>,
        val timestamp: Long
    ) : Comparable<ContinuationWrapper> {
        override fun compareTo(other: ContinuationWrapper): Int {
            return timestamp.compareTo(other.timestamp)
        }
    }

    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            val head = queue.peek()
            val winStart = System.currentTimeMillis() - window.toMillis()
            if (head == null) {
                delay(1L)
                continue
            }
            if (head.timestamp > winStart) {
                delay(head.timestamp - winStart)
                continue
            }
            sum.addAndGet(-1)
            queue.take()

            lock.withLock {
                if (pendingContinuations.isNotEmpty() && tick()) {
                    val waitingCoroutine = pendingContinuations.poll()
                    waitingCoroutine?.continuation?.resume(Unit)
                } else {
                    condition.signalAll()
                }
            }
        }
    }.invokeOnCompletion { th ->
        if (th != null) logger.error("Rate limiter release job completed", th)
    }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(SlidingWindowRateLimiter::class.java)
    }
}