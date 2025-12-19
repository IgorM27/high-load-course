package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.netty.channel.ChannelOption
import io.netty.handler.timeout.ReadTimeoutHandler
import io.netty.handler.timeout.WriteTimeoutHandler
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import org.springframework.web.reactive.function.client.awaitBody
import org.springframework.web.reactive.function.client.awaitExchange
import reactor.netty.http.HttpProtocol
import reactor.netty.http.client.HttpClient
import reactor.netty.resources.ConnectionProvider
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
        val CONNECT_TIMEOUT: Duration = Duration.ofMillis(500)
        const val MAX_RETRY_ATTEMPTS = 2
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val effectiveMaxConnections = parallelRequests.coerceAtLeast(1000)
    private val rateLimiter = SlidingWindowRateLimiter(rate = rateLimitPerSec.toLong(), window = Duration.ofSeconds(1))

    private val semaphore = java.util.concurrent.Semaphore(parallelRequests)
    private val activeRequests = AtomicInteger(0)
    private val totalProcessed = AtomicLong(0)
    private val failedRequests = AtomicLong(0)

    private val paymentDispatcher = Dispatchers.IO.limitedParallelism(parallelRequests)

    private val paymentScope = CoroutineScope(
        paymentDispatcher + SupervisorJob() + CoroutineName("payment-service-$accountName")
    )

    private val connectionProvider = ConnectionProvider.builder("payment-service-$accountName")
        .maxConnections(effectiveMaxConnections)
        .pendingAcquireMaxCount(parallelRequests * 2)
        .maxIdleTime(Duration.ofSeconds(30))
        .maxLifeTime(Duration.ofMinutes(5))
        .pendingAcquireTimeout(Duration.ofSeconds(10))
        .evictInBackground(Duration.ofSeconds(30))
        .fifo()
        .build()

    private val webClient: WebClient by lazy {
        val timeoutMs = (requestAverageProcessingTime.toMillis() * 1.2).toLong().coerceAtMost(5000)

        val httpClient = HttpClient.create(connectionProvider)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT.toMillis().toInt())
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.TCP_NODELAY, true)
            .responseTimeout(Duration.ofMillis(timeoutMs))
            .protocol(HttpProtocol.H2C, HttpProtocol.HTTP11)
            .keepAlive(true)
            .compress(false)
            .http2Settings { settings ->
                settings.maxConcurrentStreams(100000)
                    .initialWindowSize(65535 * 2)
                    .maxFrameSize(16384 * 2)
                    .headerTableSize(8192)
                    .maxHeaderListSize(8192)
            }
            .doOnConnected { conn ->
                conn.addHandlerLast(ReadTimeoutHandler(timeoutMs, TimeUnit.MILLISECONDS))
                conn.addHandlerLast(WriteTimeoutHandler(2000, TimeUnit.MILLISECONDS))
            }
            .doAfterRequest { _, _ ->
                activeRequests.incrementAndGet()
            }
            .doAfterResponseSuccess { _, _ ->
                activeRequests.decrementAndGet()
                totalProcessed.incrementAndGet()
            }

        WebClient.builder()
            .clientConnector(ReactorClientHttpConnector(httpClient))
            .codecs { configurer ->
                configurer.defaultCodecs().maxInMemorySize(256 * 1024)
            }
            .build()
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        paymentScope.launch {
            try {
                executePaymentSuspend(paymentId, amount, paymentStartedAt, deadline)
            } catch (e: Exception) {
                logger.error("[$accountName] Critical error for payment $paymentId", e)
                launch {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), UUID.randomUUID(), reason = "Critical error: ${e.message}")
                    }
                }
            }
        }
    }

    private suspend fun executePaymentSuspend(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()

        paymentScope.launch {
            try {
                paymentESService.update(paymentId) {
                    it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                }
            } catch (e: Exception) {
                logger.error("[$accountName] Failed to log submission for payment $paymentId", e)
            }
        }

        var attempt = 0
        var success = false
        var message: String? = null

        while (!success && attempt < MAX_RETRY_ATTEMPTS) {
            attempt++

            try {
                val timeUntilDeadline = deadline - now()
                if (timeUntilDeadline <= 0) {
                    message = "Deadline exceeded"
                    break
                }

                if (attempt > 1) {
                    delay((10L * attempt).coerceAtMost(100L))
                }

                if (!rateLimiter.acquireSuspend(timeUntilDeadline)) {
                    message = "Rate limit exceeded"
                    break
                }

                semaphore.acquire()
                try {
                    webClient.post()
                        .uri("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                        .header("Connection", "keep-alive")
                        .header("Accept", "application/json")
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .awaitExchange { response ->
                            if (response.statusCode().is2xxSuccessful) {
                                val body = try {
                                    mapper.readValue(response.awaitBody<String>(), ExternalSysResponse::class.java)
                                } catch (e: Exception) {
                                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "Parse error")
                                }
                                success = body.result
                                message = body.message
                            } else {
                                message = "HTTP ${response.statusCode().value()}"
                                if (response.statusCode().is5xxServerError || response.statusCode().value() == 429) {
                                    throw WebClientResponseException(
                                        response.statusCode().value(),
                                        "Error",
                                        null,
                                        null,
                                        null
                                    )
                                }
                            }
                        }
                } finally {
                    semaphore.release()
                }

                if (success) break

            } catch (e: Exception) {
                when {
                    e is SocketTimeoutException -> {
                        message = "Timeout"
                        logger.warn("[$accountName] Timeout for txId: $transactionId, attempt: $attempt")
                    }
                    isRetryableException(e) && attempt < MAX_RETRY_ATTEMPTS -> {
                        logger.warn("[$accountName] Retryable error for txId: $transactionId, attempt: $attempt: ${e.message}")
                        continue
                    }
                    else -> {
                        message = e.message ?: "Unknown error"
                        logger.error("[$accountName] Failed for txId: $transactionId, payment: $paymentId", e)
                        break
                    }
                }
            }
        }

        paymentScope.launch {
            try {
                paymentESService.update(paymentId) {
                    it.logProcessing(success, now(), transactionId, reason = message)
                }
            } catch (e: Exception) {
                logger.error("[$accountName] Failed to log processing for payment $paymentId", e)
            }
        }

        if (totalProcessed.get() % 1000 == 0L) {
            logger.info("[$accountName] Stats - processed: ${totalProcessed.get()}, failed: ${failedRequests.get()}, active: ${activeRequests.get()}, semaphore: ${semaphore.availablePermits()}")
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

private fun isRetryableException(e: Exception): Boolean {
    if (e is WebClientResponseException) {
        return e.statusCode.value() == 429 || e.statusCode.is5xxServerError || e.statusCode.value() == 408
    }
    return e is SocketTimeoutException || e is IOException
}

public fun now() = System.currentTimeMillis()