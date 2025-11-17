package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.Semaphore
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val rateLimiterFactory: PaymentRateLimiterFactory,
    private val metricsReporter: MetricsReporter
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        private val CONNECT_TIMEOUT = Duration.ofMillis(3000)
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val maxConcurrentRequests = 1000

    private val rateLimiter by lazy {
        rateLimiterFactory.getRateLimiterForAccount(accountName, (rateLimitPerSec * 1.0).toInt())
    }

    private val coroutineScope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    private val requestSemaphore = Semaphore(maxConcurrentRequests)

    private val requestChannel = Channel<PaymentRequest>(capacity = Channel.UNLIMITED)

    private val timeoutCalculator = QuantileBasedTimeoutCalculator()
    private val requestCount = AtomicInteger(0)
    private val timeoutUpdateThreshold = 33

    @Volatile private var currentReadTimeout = Duration.ofMillis(5000)

    init {
        startRequestProcessor()
    }

    private fun startRequestProcessor() {
        coroutineScope.launch {
            for (request in requestChannel) {
                launch {
                    processPaymentRequest(request)
                }
            }
        }
    }

    fun getRateLimitPerSec(): Int {
        return this.rateLimitPerSec;
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        coroutineScope.launch {
            try {
                metricsReporter.incrementOutgoing()

                val paymentRequest = PaymentRequest(paymentId, amount, paymentStartedAt, deadline)
                requestChannel.send(paymentRequest)

            } catch (e: Exception) {
                logger.error("[$accountName] Failed to submit payment $paymentId to queue", e)
            }
        }
    }

    private suspend fun processPaymentRequest(request: PaymentRequest) {
        requestSemaphore.acquire()
        try {
            executePayment(request.paymentId, request.amount, request.paymentStartedAt, request.deadline)
        } finally {
            requestSemaphore.release()
        }
    }

    private suspend fun executePayment(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        metricsReporter.updateCurrentTimeout(accountName, currentReadTimeout.toMillis())

        val transactionId = UUID.randomUUID()

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        var finalSuccess = false
        var finalMessage: String? = null

        val maxAttempts = 3
        val baseDelayMs = 300L
        val maxDelayMs = 800L

        var attempt = 0
        retryLoop@ while (true) {
            rateLimiter.tickSuspend()
            attempt += 1
            val attemptStartTime = System.currentTimeMillis()

            try {
                val timeUntilDeadline = deadline - now()
                if (timeUntilDeadline <= 0) {
                    finalSuccess = false
                    finalMessage = "Deadline exceeded"
                    break@retryLoop
                }

                val timeoutForThisRequest = minOf(currentReadTimeout.toMillis(), timeUntilDeadline)

                val request = Request.Builder().run {
                    url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    post(emptyBody)
                }.build()

                val requestClient = OkHttpClient.Builder()
                    .connectTimeout(CONNECT_TIMEOUT)
                    .readTimeout(Duration.ofMillis(timeoutForThisRequest))
                    .callTimeout(Duration.ofMillis(timeoutForThisRequest + 1000))
                    .build()

                val response = withContext(Dispatchers.IO) {
                    requestClient.newCall(request).execute()
                }

                response.use { httpResponse ->
                    val code = httpResponse.code
                    if (code == 429 || code == 500 || code == 502 || code == 503  || code == 504) {
                        val retryAfter = httpResponse.header("Retry-After")?.toLongOrNull()?.times(1000)
                        val delay = retryAfter ?: computeDelayMillis(attempt, baseDelayMs, maxDelayMs)
                        if (!shouldRetry(attempt, maxAttempts, deadline, delay)) {
                            finalSuccess = false
                            finalMessage = "HTTP $code"
                            break@retryLoop
                        }
                        logger.warn("[$accountName] HTTP $code for $paymentId (attempt $attempt), retrying in ${delay}ms")
                        metricsReporter.incrementRetry()
                        delay(adjustDelayToDeadline(delay, deadline))
                        return@use // continue to next attempt
                    }

                    val body = try {
                        mapper.readValue(httpResponse.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${httpResponse.code}, reason: ${httpResponse.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    finalSuccess = body.result
                    finalMessage = body.message

                    break@retryLoop
                }
            } catch (e: Exception) {
                val retryable = e is SocketTimeoutException || e is IOException
                if (retryable) {
                    val delay = computeDelayMillis(attempt, baseDelayMs, maxDelayMs)
                    if (!shouldRetry(attempt, maxAttempts, deadline, delay)) {
                        logger.error("[$accountName] Payment retry attempts exhausted for txId: $transactionId, payment: $paymentId. Last error: ${e.message}")
                        finalSuccess = false
                        finalMessage = e.message ?: "Request timeout."
                        break@retryLoop
                    }
                    logger.warn("[$accountName] Transient error on attempt $attempt for payment $paymentId: ${e.message}. Retrying in ${delay}ms")
                    metricsReporter.incrementRetry()
                    delay(adjustDelayToDeadline(delay, deadline))
                } else {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    finalSuccess = false
                    finalMessage = e.message
                    break@retryLoop
                }
            } finally {
                val latency = System.currentTimeMillis() - attemptStartTime
                timeoutCalculator.addLatencyMeasurement(latency)

                val count = requestCount.incrementAndGet()
                if (count % timeoutUpdateThreshold == 0) {
                    updateTimeouts()
                }
            }

            if (finalSuccess || attempt >= maxAttempts) {
                break@retryLoop
            }
        }

        paymentESService.update(paymentId) {
            it.logProcessing(finalSuccess, now(), transactionId, reason = finalMessage)
        }

        if (finalSuccess) {
            metricsReporter.incrementCompleted()
        } else {
            metricsReporter.incrementFailed()
        }
    }

    private suspend fun updateTimeouts() {
        val newTimeout = timeoutCalculator.calculateOptimalTimeout()
        if (newTimeout != currentReadTimeout) {
            currentReadTimeout = newTimeout
            metricsReporter.updateCurrentTimeout(accountName, currentReadTimeout.toMillis())
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    fun close() {
        coroutineScope.cancel()
    }
}

data class PaymentRequest(
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    val deadline: Long
)

fun now() = System.currentTimeMillis()

private fun computeDelayMillis(attempt: Int, baseDelayMs: Long, maxDelayMs: Long): Long {
    val exp = baseDelayMs * (1L shl (attempt - 1).coerceAtLeast(1))
    val capped = exp.coerceAtMost(maxDelayMs)
    val jitter = 0.8 + Math.random() * 0.4
    return (capped * jitter).toLong()
}

private fun shouldRetry(attempt: Int, maxAttempts: Int, deadline: Long, delayMs: Long): Boolean {
    if (attempt >= maxAttempts) return false
    val remaining = deadline - now()
    return remaining > delayMs
}

private fun adjustDelayToDeadline(delayMs: Long, deadline: Long): Long {
    val remaining = deadline - now()
    return if (remaining <= 0) 0 else Math.min(delayMs, remaining)
}