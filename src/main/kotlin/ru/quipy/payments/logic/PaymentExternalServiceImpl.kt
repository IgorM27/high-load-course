package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
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
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger


// Advice: always treat time as a Duration
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
    private val rateLimiter by lazy {
        rateLimiterFactory.getRateLimiterForAccount(accountName, (rateLimitPerSec * 1.0).toInt())
    }
    private val paymentExecutor = Executors.newFixedThreadPool(parallelRequests)

    private val timeoutCalculator = QuantileBasedTimeoutCalculator()
    private val requestCount = AtomicInteger(0)
    private val timeoutUpdateThreshold = 33

    @Volatile private var currentReadTimeout = Duration.ofMillis(5000)

    fun getRateLimitPerSec(): Int {
        return this.rateLimitPerSec;
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        paymentExecutor.submit {
            try {
                metricsReporter.incrementOutgoing()
                rateLimiter.tickBlocking()

                executePayment(paymentId, amount, paymentStartedAt, deadline)
            } catch (e: Exception) {
                logger.error("[$accountName] Failed to process payment $paymentId", e)
            }
        }
    }

    private fun executePayment(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        // Expose current timeout as gauge for observability
        metricsReporter.updateCurrentTimeout(accountName, currentReadTimeout.toMillis())

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
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
        while (true) {
            attempt += 1
            val attemptStartTime = System.currentTimeMillis()

            try {
                val timeUntilDeadline = deadline - now()
                if (timeUntilDeadline <= 0) {
                    finalSuccess = false
                    finalMessage = "Deadline exceeded"
                    break
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

                requestClient.newCall(request).execute().use { response ->
                    val code = response.code
                    // Retry only on transient HTTP statuses
                    if (code == 429 || code == 500 || code == 502 || code == 503  || code == 504) {
                        val retryAfter = response.header("Retry-After")?.toLongOrNull()?.times(1000)
                        val delay = retryAfter ?: computeDelayMillis(attempt, baseDelayMs, maxDelayMs)
                        if (!shouldRetry(attempt, maxAttempts, deadline, delay)) {
                            finalSuccess = false
                            finalMessage = "HTTP $code"
                            break
                        }
                        logger.warn("[$accountName] HTTP $code for $paymentId (attempt $attempt), retrying in ${delay}ms")
                        metricsReporter.incrementRetry()
                        Thread.sleep(adjustDelayToDeadline(delay, deadline))
                        continue
                    }

                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    finalSuccess = body.result
                    finalMessage = body.message
                }
            } catch (e: Exception) {
                val retryable = e is SocketTimeoutException || e is IOException
                if (retryable) {
                    val delay = computeDelayMillis(attempt, baseDelayMs, maxDelayMs)
                    if (!shouldRetry(attempt, maxAttempts, deadline, delay)) {
                        logger.error("[$accountName] Payment retry attempts exhausted for txId: $transactionId, payment: $paymentId. Last error: ${e.message}")
                        finalSuccess = false
                        finalMessage = e.message ?: "Request timeout."
                        break
                    }
                    logger.warn("[$accountName] Transient error on attempt $attempt for payment $paymentId: ${e.message}. Retrying in ${delay}ms")
                    metricsReporter.incrementRetry()
                    Thread.sleep(adjustDelayToDeadline(delay, deadline))
                    continue
                } else {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    finalSuccess = false
                    finalMessage = e.message
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
                break
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

    private fun updateTimeouts() {
        val newTimeout = timeoutCalculator.calculateOptimalTimeout()
        if (newTimeout != currentReadTimeout) {
            currentReadTimeout = newTimeout
            metricsReporter.updateCurrentTimeout(accountName, currentReadTimeout.toMillis())
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

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