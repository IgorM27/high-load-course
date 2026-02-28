package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.*
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.slf4j.LoggerFactory
import io.netty.channel.ChannelOption
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException.TooManyRequests
import org.springframework.web.reactive.function.client.bodyToMono
import reactor.netty.http.HttpProtocol
import reactor.netty.http.client.HttpClient
import reactor.netty.resources.ConnectionProvider
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeoutException

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
    val mapper = ObjectMapper().registerKotlinModule()

    val connectionProvider: ConnectionProvider = ConnectionProvider.builder("payment-provider")
        .maxConnections(parallelRequests)
        .pendingAcquireMaxCount(parallelRequests)
        .pendingAcquireTimeout(Duration.ofSeconds(120))
        .maxIdleTime(Duration.ofSeconds(60))
        .build()

    val sharedHttpClient: HttpClient = HttpClient.create(connectionProvider)
        .protocol(HttpProtocol.H2C)
        .responseTimeout(Duration.ofMillis(1000))
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 100)

    private val sharedDispatcher = Executors.newFixedThreadPool(64).asCoroutineDispatcher()

    val paymentScope = CoroutineScope(SupervisorJob() + sharedDispatcher)

    private val rateLimiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(),
        Duration.ofSeconds(1)
    )

    private val semaphore = Semaphore(parallelRequests)

    init {
        logger.warn("[$accountName] Initialized with rateLimitPerSec=$rateLimitPerSec, parallelRequests=$parallelRequests")
    }

    private val webClient: WebClient = WebClient.builder()
        .baseUrl("http://$paymentProviderHostPort")
        .clientConnector(ReactorClientHttpConnector(sharedHttpClient))
        .build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        paymentScope.launch {
            executePaymentReactive(paymentId, amount, transactionId)
        }
    }

    private suspend fun executePaymentReactive(paymentId: UUID, amount: Int, transactionId: UUID) {
        semaphore.withPermit {
            rateLimiter.tickSuspending()

            var retryCount = 0
            val maxRetries = 3
            var success = false

            while (retryCount < maxRetries && !success) {
                try {
                    val responseBody = webClient.post()
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
                        .retrieve()
                        .bodyToMono<String>()
                        .awaitSingle()

                    val body = try {
                        mapper.readValue(responseBody, ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] Failed to parse response for payment $paymentId: $responseBody")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    logger.info("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                    success = true

                } catch (e: TooManyRequests) {
                    retryCount++
                    logger.warn("[$accountName] Payment got 429 for txId: $transactionId, payment: $paymentId, retry $retryCount/$maxRetries")
                    if (retryCount < maxRetries) {
                        // Простая задержка перед повторной попыткой
                        delay(100L * retryCount)
                    } else {
                        logger.error("[$accountName] Payment failed after $maxRetries retries for txId: $transactionId, payment: $paymentId")
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "TooManyRequests after $maxRetries retries")
                        }
                        throw e
                    }

                } catch (e: Exception) {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                    throw e
                }
            }
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()