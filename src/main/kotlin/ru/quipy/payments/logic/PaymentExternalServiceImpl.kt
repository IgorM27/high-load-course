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
import reactor.netty.http.client.HttpClientResponse
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

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val mapper = ObjectMapper().registerKotlinModule()

        val CONNECT_TIMEOUT: Duration = Duration.ofSeconds(1)
        const val MAX_RETRY_ATTEMPTS = 3

        private const val HTTP2_MAX_CONCURRENT_STREAMS = 10000
        private const val HTTP2_INITIAL_WINDOW_SIZE = 65535
        private const val HTTP2_MAX_FRAME_SIZE = 16384
        private const val HTTP2_HEADER_TABLE_SIZE = 4096
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val effectiveMaxConnections = calculateOptimalConnections()

    private fun calculateOptimalConnections(): Int {
        val calculated = minOf(
            parallelRequests,
            (rateLimitPerSec * requestAverageProcessingTime.toSeconds() * 1.2).toInt()
        )

        return calculated;
    }

    private val rateLimiter: SlidingWindowRateLimiter by lazy {
        SlidingWindowRateLimiter(rate = rateLimitPerSec.toLong(), window = Duration.ofSeconds(1))
    }

    private val esDispatcher = Dispatchers.IO.limitedParallelism(
        parallelism = minOf(effectiveMaxConnections * 2, 256).coerceAtLeast(8)
    )

    private val paymentScope = CoroutineScope(
        Dispatchers.IO.limitedParallelism(parallelRequests.coerceAtMost(1000)) +
                SupervisorJob() +
                CoroutineName("payment-service-$accountName")
    )

    private val connectionProvider = ConnectionProvider.builder("payment-service-$accountName")
        .maxConnections(effectiveMaxConnections)
        .maxIdleTime(Duration.ofSeconds(30))
        .maxLifeTime(Duration.ofMinutes(5))
        .pendingAcquireTimeout(Duration.ofSeconds(30))
        .pendingAcquireMaxCount(-1)
        .evictInBackground(Duration.ofSeconds(60))
        .metrics(true)
        .build()

    private val activeRequests = AtomicInteger(0)

    private val webClient: WebClient by lazy {
        val timeoutMs = (requestAverageProcessingTime.toMillis() * 1.5).toLong()

        val httpClient = HttpClient.create(connectionProvider)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT.toMillis().toInt())
            .option(ChannelOption.SO_KEEPALIVE, true)
            .responseTimeout(Duration.ofMillis(timeoutMs))
            .protocol(HttpProtocol.H2C, HttpProtocol.HTTP11)
            .http2Settings { settings ->
                settings
                    .maxConcurrentStreams(HTTP2_MAX_CONCURRENT_STREAMS.toLong())
                    .initialWindowSize(HTTP2_INITIAL_WINDOW_SIZE)
                    .maxFrameSize(HTTP2_MAX_FRAME_SIZE)
                    .headerTableSize(HTTP2_HEADER_TABLE_SIZE.toLong())
            }
            .keepAlive(true)
            .compress(true)
            .metrics(true, { uri -> uri })
            .doOnConnected { conn ->
                conn.addHandlerLast(ReadTimeoutHandler(timeoutMs, TimeUnit.MILLISECONDS))
                conn.addHandlerLast(WriteTimeoutHandler(CONNECT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
            }
            .doAfterRequest { req, conn ->
                val count = activeRequests.incrementAndGet()
                if (count % 100 == 0) {
                    logger.debug("[$accountName] Active HTTP requests: $count")
                }
            }
            .doAfterResponseSuccess { res, conn ->
                activeRequests.decrementAndGet()
            }

        WebClient.builder()
            .clientConnector(ReactorClientHttpConnector(httpClient))
            .codecs { configurer ->
                configurer.defaultCodecs().maxInMemorySize(1024 * 1024) // 1MB буфер
            }
            .build()
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        paymentScope.launch {
            executePaymentSuspend(paymentId, amount, paymentStartedAt, deadline)
        }
    }

    private suspend fun executePaymentSuspend(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()

        paymentScope.launch(esDispatcher) {
            try {
                paymentESService.update(paymentId) {
                    it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                }
            } catch (e: Exception) {
                logger.error("[$accountName] Failed to log submission for payment $paymentId", e)
            }
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        var attempt = 0
        var message: String? = null
        var success = false

        while (!success && attempt++ < MAX_RETRY_ATTEMPTS) {
            try {
                if (attempt > 1) {
                    val backoffDelay = (50L * (1L shl (attempt - 2))).coerceAtMost(5000L)
                    delay(backoffDelay)
                    logger.info("[$accountName] Retry attempt $attempt for payment $paymentId after ${backoffDelay}ms")
                }

                val timeUntilDeadline = deadline - now()
                if (timeUntilDeadline <= 0) {
                    message = "Deadline exceeded"
                    break
                }

                if (!rateLimiter.acquireSuspend(timeUntilDeadline.coerceAtMost(5000L))) {
                    message = "Rate limit exceeded or deadline too close"
                    break
                }

                webClient.post()
                    .uri("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    .header("Connection", "keep-alive") // Явно указываем keep-alive
                    .header("Accept", "application/json")
                    .awaitExchange { response ->
                        val statusCode = response.statusCode()
                        if (statusCode.is2xxSuccessful) {
                            val body = try {
                                mapper.readValue(response.awaitBody<String>(), ExternalSysResponse::class.java)
                            } catch (e: Exception) {
                                logger.error("[$accountName] [ERROR] Failed to parse response for txId: $transactionId, payment: $paymentId", e)
                                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                            }

                            logger.info("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                            message = body.message
                            success = body.result
                        } else {
                            val errorBody = try {
                                response.awaitBody<String>()
                            } catch (e: Exception) {
                                "Failed to read error response: ${e.message}"
                            }
                            message = "HTTP ${statusCode.value()}: $errorBody"
                            logger.warn("[$accountName] Payment failed with HTTP ${statusCode.value()} for txId: $transactionId, payment: $paymentId: $errorBody")
                        }
                    }

            } catch (e: Exception) {
                when {
                    e is SocketTimeoutException -> {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId, attempt: $attempt", e)
                        message = "Request timeout after ${requestAverageProcessingTime.toMillis()}ms"
                    }
                    isRetryableException(e) -> {
                        logger.warn("[$accountName] Retryable error for txId: $transactionId, payment: $paymentId, attempt: $attempt", e)
                        if (attempt < MAX_RETRY_ATTEMPTS) {
                            continue
                        } else {
                            message = "Max retry attempts exceeded: ${e.message}"
                        }
                    }
                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId, attempt: $attempt", e)
                        message = e.message ?: "Unknown error"
                        break
                    }
                }
            }
        }

        paymentScope.launch(esDispatcher) {
            try {
                paymentESService.update(paymentId) {
                    it.logProcessing(success, now(), transactionId, reason = message)
                }
            } catch (e: Exception) {
                logger.error("[$accountName] Failed to log processing for payment $paymentId", e)
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

private fun isRetryableException(e: Exception): Boolean {
    return when {
        e is WebClientResponseException -> {
            e.statusCode.value() == 429 ||
                    e.statusCode.value() == 408 ||
                    e.statusCode.is5xxServerError
        }
        e is SocketTimeoutException -> true
        e is IOException -> true
        e.cause is SocketTimeoutException -> true
        e.cause is IOException -> true
        else -> false
    }
}

public fun now() = System.currentTimeMillis()