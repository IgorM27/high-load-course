package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.*

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val dbScope: CoroutineScope,
    private val circuitBreaker: CircuitBreaker
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName

    private val rateLimiter = SlidingWindowRateLimiter(properties.rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindow(properties.parallelRequests, false)
    private val scheduler = Executors.newScheduledThreadPool(100)

    private val httpExecutor = ThreadPoolExecutor(
        maxOf(100, properties.parallelRequests / 10),
        maxOf(100, properties.parallelRequests / 10),
        60L, TimeUnit.SECONDS,
        LinkedBlockingQueue(properties.parallelRequests * 2),
        Executors.defaultThreadFactory(),
        CallerBlockingRejectedExecutionHandler(Duration.ofSeconds(5))
    )

    private val client: HttpClient = HttpClient.newBuilder()
        .executor(httpExecutor).version(HttpClient.Version.HTTP_2).build()

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

        if (!circuitBreaker.tryAcquirePermission()) {
            logger.warn("[$accountName] Circuit breaker open, rejecting $paymentId")
            logProcessingAsync(paymentId, false, transactionId, "Circuit breaker open")
            return
        }

        if (!rateLimiter.tickBlocking(Duration.ofMillis(deadline - now()))) {
            logProcessingAsync(paymentId, false, transactionId, "Rate limit exceeded")
            return
        }

        if (!ongoingWindow.acquire(deadline - now(), TimeUnit.MILLISECONDS)) {
            logger.warn("[$accountName] Semaphore timeout for $paymentId")
            logProcessingAsync(paymentId, false, transactionId, "Semaphore timeout")
            return
        }

        val cbStart = now()
        val futures = mutableListOf(sendRequest(transactionId, paymentId, amount))

        for (i in 1..2) {
            scheduler.schedule({
                if (futures.none { it.isDone }) futures.add(sendRequest(transactionId, paymentId, amount))
            }, 160L * i, TimeUnit.MILLISECONDS)
        }

        CompletableFuture.anyOf(*futures.toTypedArray())
            .thenApply { it as HttpResponse<String> }
            .thenAccept { response ->
                val elapsed = now() - cbStart
                val body = try {
                    mapper.readValue(response.body(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] Parse error for txId: $transactionId, payment: $paymentId", e)
                    circuitBreaker.onError(elapsed, TimeUnit.MILLISECONDS, e)
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                if (body.result) circuitBreaker.onSuccess(elapsed, TimeUnit.MILLISECONDS)
                else circuitBreaker.onError(elapsed, TimeUnit.MILLISECONDS, RuntimeException(body.message ?: "Rejected"))

                logger.warn("[$accountName] Result for txId: $transactionId, payment: $paymentId, ok: ${body.result}")
                logProcessingAsync(paymentId, body.result, transactionId, body.message)
                ongoingWindow.release()
            }
            .exceptionally { ex ->
                val rootCause = ex.cause ?: ex
                logger.error("[$accountName] Failed txId: $transactionId, payment: $paymentId", rootCause)
                circuitBreaker.onError(now() - cbStart, TimeUnit.MILLISECONDS, rootCause)
                logProcessingAsync(paymentId, false, transactionId, rootCause.message)
                ongoingWindow.release()
                null
            }
    }

    private fun sendRequest(txId: UUID, paymentId: UUID, amount: Int): CompletableFuture<HttpResponse<String>> {
        val request = HttpRequest.newBuilder()
            .uri(URI("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$txId&paymentId=$paymentId&amount=$amount"))
            .timeout(Duration.ofMillis(1500))
            .header("x-idempotency-key", txId.toString())
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
    }

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