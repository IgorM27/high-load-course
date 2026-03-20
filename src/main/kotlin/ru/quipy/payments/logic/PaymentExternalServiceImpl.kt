package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import kotlinx.coroutines.*
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.client.WebClient
import ru.quipy.common.utils.SlidingWindowRateLimiter
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
    private val dbScope: CoroutineScope,
    private val circuitBreaker: CircuitBreaker,
    private val webClient: WebClient
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
        const val MAX_HEDGE_ATTEMPTS = 2
        const val HEDGE_DELAY_MS = 160L
        const val REQUEST_TIMEOUT_MS = 400L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName

    private val rateLimiter = SlidingWindowRateLimiter(properties.rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val semaphore = Semaphore(properties.parallelRequests)

    private fun logProcessingAsync(paymentId: UUID, success: Boolean, txId: UUID, reason: String?) {
        val time = now()
        dbScope.launch {
            retryOnConflict { paymentESService.update(paymentId) { it.logProcessing(success, time, txId, reason = reason) } }
        }
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()
        val startedAt = now()

        dbScope.launch {
            retryOnConflict {
                paymentESService.update(paymentId) {
                    it.logSubmission(true, transactionId, startedAt, Duration.ofMillis(startedAt - paymentStartedAt))
                }
            }
        }

        logger.info("[$accountName] Submit: $paymentId, txId: $transactionId")

        dbScope.launch {
            executeWithProtection(paymentId, amount, deadline, transactionId)
        }
    }

    private suspend fun executeWithProtection(paymentId: UUID, amount: Int, deadline: Long, transactionId: UUID) {
        if (!circuitBreaker.tryAcquirePermission()) {
            logger.warn("[$accountName] Circuit breaker open, rejecting $paymentId")
            logProcessingAsync(paymentId, false, transactionId, "Circuit breaker open")
            return
        }

        if (!rateLimiter.tickBlocking(Duration.ofMillis(deadline - now()))) {
            logProcessingAsync(paymentId, false, transactionId, "Rate limit exceeded")
            return
        }

        val cbStart = now()
        try {
            val response = semaphore.withPermit {
                hedgedRequest(transactionId, paymentId, amount)
            }

            val elapsed = now() - cbStart
            val body = response.body
            if (body != null && body.result) {
                circuitBreaker.onSuccess(elapsed, TimeUnit.MILLISECONDS)
            } else {
                circuitBreaker.onError(elapsed, TimeUnit.MILLISECONDS, RuntimeException(body?.message ?: "Rejected"))
            }

            logger.warn("[$accountName] Result for txId: $transactionId, payment: $paymentId, ok: ${body?.result}")
            logProcessingAsync(paymentId, body?.result == true, transactionId, body?.message)
        } catch (e: Exception) {
            logger.error("[$accountName] Failed txId: $transactionId, payment: $paymentId", e)
            circuitBreaker.onError(now() - cbStart, TimeUnit.MILLISECONDS, e)
            logProcessingAsync(paymentId, false, transactionId, e.message)
        }
    }

    private suspend fun hedgedRequest(txId: UUID, paymentId: UUID, amount: Int) = coroutineScope {
        val result = CompletableDeferred<org.springframework.http.ResponseEntity<ExternalSysResponse?>>()

        val jobs = mutableListOf<Job>()
        for (attempt in 0..MAX_HEDGE_ATTEMPTS) {
            val job = launch {
                if (attempt > 0) delay(HEDGE_DELAY_MS * attempt)
                if (result.isCompleted) return@launch
                try {
                    val response = withTimeout(REQUEST_TIMEOUT_MS) { sendRequest(txId, paymentId, amount) }
                    result.complete(response)
                } catch (e: Exception) {
                    if (attempt == MAX_HEDGE_ATTEMPTS.toInt() && !result.isCompleted) {
                        result.completeExceptionally(e)
                    }
                }
            }
            jobs.add(job)
        }

        try {
            result.await()
        } finally {
            jobs.forEach { it.cancel() }
        }
    }

    private suspend fun sendRequest(txId: UUID, paymentId: UUID, amount: Int) =
        webClient.post()
            .uri("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$txId&paymentId=$paymentId&amount=$amount")
            .header("x-idempotency-key", txId.toString())
            .accept(MediaType.APPLICATION_JSON)
            .retrieve()
            .toEntity(ExternalSysResponse::class.java)
            .awaitSingle()

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
    override fun maxRateLimit() = properties.rateLimitPerSec
}

private suspend fun retryOnConflict(block: suspend () -> Unit) {
    while (true) {
        try { block(); return } catch (_: IllegalArgumentException) { delay(10) }
    }
}

public fun now() = System.currentTimeMillis()