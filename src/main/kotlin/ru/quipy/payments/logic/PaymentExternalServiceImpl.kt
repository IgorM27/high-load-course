package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.netty.channel.ChannelOption
import io.netty.handler.timeout.ReadTimeoutHandler
import io.netty.handler.timeout.WriteTimeoutHandler
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatusCode
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import org.springframework.web.reactive.function.client.awaitBody
import org.springframework.web.reactive.function.client.awaitExchange
import reactor.netty.http.client.HttpClient
import reactor.netty.resources.ConnectionProvider
import ru.quipy.common.utils.NonBlockingSlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit

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
        val mapper = ObjectMapper().registerKotlinModule()

        private val CONNECT_TIMEOUT = Duration.ofMillis(3000)
        private const val MAX_CONNECTIONS = 25000
        private const val PENDING_ACQUIRE_TIMEOUT_SEC = 60L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter: NonBlockingSlidingWindowRateLimiter by lazy {
        rateLimiterFactory.getNonBlockingRateLimiter(accountName, (rateLimitPerSec * 1.0).toInt())
    }

    private val paymentScope = CoroutineScope(
        Dispatchers.IO + SupervisorJob() + CoroutineName("PaymentAdapter-$accountName")
    )

    private val currentReadTimeout: Duration = computeStaticTimeout()

    private val connectionProvider = ConnectionProvider.builder("payment-provider-$accountName")
        .maxConnections(MAX_CONNECTIONS)
        .pendingAcquireTimeout(Duration.ofSeconds(PENDING_ACQUIRE_TIMEOUT_SEC))
        .pendingAcquireMaxCount(-1)
        .maxIdleTime(Duration.ofSeconds(30))
        .maxLifeTime(Duration.ofMinutes(5))
        .build()

    private val webClient: WebClient by lazy {
        val httpClient = HttpClient.create(connectionProvider)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT.toMillis().toInt())
            .responseTimeout(currentReadTimeout)
            .doOnConnected { conn ->
                conn.addHandlerLast(ReadTimeoutHandler(currentReadTimeout.toMillis(), TimeUnit.MILLISECONDS))
                conn.addHandlerLast(WriteTimeoutHandler(CONNECT_TIMEOUT.toMillis().toLong(), TimeUnit.MILLISECONDS))
            }

        WebClient.builder()
            .baseUrl("http://$paymentProviderHostPort")
            .clientConnector(ReactorClientHttpConnector(httpClient))
            .build()
    }

    fun getRateLimitPerSec(): Int {
        return this.rateLimitPerSec
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        paymentScope.launch {
            try {
                metricsReporter.incrementOutgoing()
                executePaymentSuspend(paymentId, amount, paymentStartedAt, deadline)
            } catch (e: Exception) {
                logger.error("[$accountName] Failed to process payment $paymentId", e)
            }
        }
    }

    private suspend fun executePaymentSuspend(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        metricsReporter.updateCurrentTimeout(accountName, currentReadTimeout.toMillis())

        val transactionId = UUID.randomUUID()

        withContext(Dispatchers.IO) {
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        val maxAttempts = 3
        val baseDelayMs = 300L
        val maxDelayMs = 800L

        suspend fun finalizeResult(success: Boolean, message: String?) {
            withContext(Dispatchers.IO) {
                paymentESService.update(paymentId) {
                    it.logProcessing(success, now(), transactionId, reason = message)
                }
            }
            if (success) {
                metricsReporter.incrementCompleted()
            } else {
                metricsReporter.incrementFailed()
            }
        }

        suspend fun attempt(attemptNumber: Int) {
            val timeUntilDeadline = deadline - now()
            if (timeUntilDeadline <= 0) {
                finalizeResult(false, "Deadline exceeded")
                return
            }

            val timeoutForThisRequest = minOf(currentReadTimeout.toMillis(), timeUntilDeadline)

            if (!rateLimiter.acquireSuspendWithTimeout(timeUntilDeadline)) {
                finalizeResult(false, "Rate limit timeout exceeded")
                return
            }

            try {
                val response = webClient.post()
                    .uri { uriBuilder ->
                        uriBuilder.path("/external/process")
                            .queryParam("serviceName", serviceName)
                            .queryParam("token", token)
                            .queryParam("accountName", accountName)
                            .queryParam("transactionId", transactionId)
                            .queryParam("paymentId", paymentId)
                            .queryParam("amount", amount)
                            .build()
                    }
                    .awaitExchange { response ->
                        val statusCode = response.statusCode()
                        
                        when {
                            statusCode.is2xxSuccessful -> {
                                try {
                                    val body = response.awaitBody<String>()
                                    val externalResponse = mapper.readValue(body, ExternalSysResponse::class.java)
                                    PaymentResult.Success(externalResponse)
                                } catch (e: Exception) {
                                    logger.error("[$accountName] [ERROR] Failed to parse response for txId: $transactionId, payment: $paymentId", e)
                                    PaymentResult.Success(ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message))
                                }
                            }
                            statusCode.value() == 429 || statusCode.is5xxServerError -> {
                                val retryAfter = response.headers().header("Retry-After").firstOrNull()?.toLongOrNull()?.times(1000)
                                PaymentResult.Retryable(statusCode.value(), retryAfter)
                            }
                            else -> {
                                PaymentResult.Failed("HTTP ${statusCode.value()}")
                            }
                        }
                    }

                when (response) {
                    is PaymentResult.Success -> {
                        val body = response.data
                        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                        finalizeResult(body.result, body.message)
                    }
                    is PaymentResult.Retryable -> {
                        val retryDelay = response.retryAfter ?: computeDelayMillis(attemptNumber, baseDelayMs, maxDelayMs)
                        if (!shouldRetry(attemptNumber, maxAttempts, deadline, retryDelay)) {
                            val reason = "HTTP ${response.statusCode}"
                            logger.warn("[$accountName] Attempts exhausted for $paymentId, last status: $reason")
                            finalizeResult(false, reason)
                            return
                        }
                        logger.warn("[$accountName] HTTP ${response.statusCode} for $paymentId (attempt $attemptNumber), retrying in ${retryDelay}ms")
                        metricsReporter.incrementRetry()
                        val adjustedDelay = adjustDelayToDeadline(retryDelay, deadline)
                        if (adjustedDelay <= 0L) {
                            finalizeResult(false, "Deadline exceeded")
                            return
                        }
                        delay(adjustedDelay)
                        attempt(attemptNumber + 1)
                    }
                    is PaymentResult.Failed -> {
                        finalizeResult(false, response.reason)
                    }
                }

            } catch (e: WebClientResponseException) {
                val statusCode = e.statusCode.value()
                if (statusCode == 429 || e.statusCode.is5xxServerError) {
                    handleRetryableError(attemptNumber, maxAttempts, deadline, baseDelayMs, maxDelayMs, paymentId, transactionId, e.message ?: "HTTP $statusCode") {
                        attempt(attemptNumber + 1)
                    }
                } else {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    finalizeResult(false, e.message)
                }
            } catch (e: io.netty.handler.timeout.TimeoutException) {
                handleRetryableError(attemptNumber, maxAttempts, deadline, baseDelayMs, maxDelayMs, paymentId, transactionId, "Request timeout") {
                    attempt(attemptNumber + 1)
                }
            } catch (e: io.netty.handler.timeout.ReadTimeoutException) {
                handleRetryableError(attemptNumber, maxAttempts, deadline, baseDelayMs, maxDelayMs, paymentId, transactionId, "Read timeout") {
                    attempt(attemptNumber + 1)
                }
            } catch (e: java.util.concurrent.TimeoutException) {
                handleRetryableError(attemptNumber, maxAttempts, deadline, baseDelayMs, maxDelayMs, paymentId, transactionId, "Timeout") {
                    attempt(attemptNumber + 1)
                }
            } catch (e: Exception) {
                if (isRetryableException(e)) {
                    handleRetryableError(attemptNumber, maxAttempts, deadline, baseDelayMs, maxDelayMs, paymentId, transactionId, e.message ?: "IO Error") {
                        attempt(attemptNumber + 1)
                    }
                } else {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    finalizeResult(false, e.message)
                }
            }
        }

        attempt(1)
    }

    private fun isRetryableException(e: Exception): Boolean {
        return e is java.net.SocketTimeoutException ||
               e is java.io.IOException ||
               e is reactor.netty.http.client.PrematureCloseException ||
               e.cause?.let { isRetryableException(it as? Exception ?: return@let false) } ?: false
    }

    private suspend fun handleRetryableError(
        attemptNumber: Int,
        maxAttempts: Int,
        deadline: Long,
        baseDelayMs: Long,
        maxDelayMs: Long,
        paymentId: UUID,
        transactionId: UUID,
        errorMessage: String,
        retryAction: suspend () -> Unit
    ) {
        val retryDelay = computeDelayMillis(attemptNumber, baseDelayMs, maxDelayMs)
        if (!shouldRetry(attemptNumber, maxAttempts, deadline, retryDelay)) {
            logger.error("[$accountName] Payment retry attempts exhausted for txId: $transactionId, payment: $paymentId. Last error: $errorMessage")
            withContext(Dispatchers.IO) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = errorMessage)
                }
            }
            metricsReporter.incrementFailed()
            return
        }
        logger.warn("[$accountName] Transient error on attempt $attemptNumber for payment $paymentId: $errorMessage. Retrying in ${retryDelay}ms")
        metricsReporter.incrementRetry()
        val adjustedDelay = adjustDelayToDeadline(retryDelay, deadline)
        if (adjustedDelay <= 0L) {
            withContext(Dispatchers.IO) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded")
                }
            }
            metricsReporter.incrementFailed()
            return
        }
        delay(adjustedDelay)
        retryAction()
    }

    private fun computeStaticTimeout(): Duration {
        val baseMs = requestAverageProcessingTime.toMillis().coerceAtLeast(1000L)
        val computed = (baseMs * 1.5).toLong()
        val bounded = computed.coerceIn(1_000L, 20_000L)
        return Duration.ofMillis(bounded)
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

private sealed class PaymentResult {
    data class Success(val data: ExternalSysResponse) : PaymentResult()
    data class Retryable(val statusCode: Int, val retryAfter: Long?) : PaymentResult()
    data class Failed(val reason: String) : PaymentResult()
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
