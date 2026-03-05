package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit


class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val dbScope: CoroutineScope
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val MAX_ATTEMPTS = 2
    private val HEDGE_DELAY = 160L
    private val TIMEOUT = Duration.ofMillis(1500)

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindow(parallelRequests, false)

    private val scheduler = Executors.newScheduledThreadPool(100)
    private val httpThreadPoolSize = maxOf(100, parallelRequests / 10)
    private val httpExecutor = ThreadPoolExecutor(
        httpThreadPoolSize,
        httpThreadPoolSize,
        60L,
        TimeUnit.SECONDS,
        LinkedBlockingQueue(parallelRequests * 2),
        Executors.defaultThreadFactory(),
        CallerBlockingRejectedExecutionHandler(Duration.ofSeconds(5))
    )

    private val client: HttpClient = HttpClient.newBuilder()
        .executor(httpExecutor)
        .version(HttpClient.Version.HTTP_2)
        .build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        val startedAt = now()
        dbScope.launch {
            while (true) {
                try {
                    paymentESService.update(paymentId) {
                        it.logSubmission(
                            success = true,
                            transactionId,
                            startedAt,
                            Duration.ofMillis(startedAt - paymentStartedAt)
                        )
                    }
                    break
                } catch (_: java.lang.IllegalArgumentException) {
                    delay(10)
                }
            }
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")
        performPaymentAsync(paymentId, amount, paymentStartedAt, deadline, transactionId)

    }

    private fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, transactionId: UUID) {
        if (!rateLimiter.tickBlocking(Duration.ofMillis(deadline - now()))) {
            val currentTime = now()
            dbScope.launch {
                while (true) {
                    try {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, currentTime, transactionId, reason = "Rate limit exceed")
                        }
                        break
                    } catch (_: java.lang.IllegalArgumentException) {
                        delay(10)
                    }
                }
            }
            return
        }

        val timeToBlock = deadline - System.currentTimeMillis()
        val acquired = ongoingWindow.acquire(timeToBlock, TimeUnit.MILLISECONDS)
        if (!acquired) {
            logger.warn("[$accountName] Timeout acquiring semaphore for payment $paymentId")
            val currentTime = now()
            dbScope.launch {
                while (true) {
                    try {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, currentTime, transactionId, reason = "Semaphore timeout")
                        }
                        break
                    } catch (_: java.lang.IllegalArgumentException) {
                        delay(10)
                    }
                }
            }
            return
        }

        val futures = mutableListOf<CompletableFuture<HttpResponse<String>>>()

        var request = makeRequest(transactionId, paymentId, amount)
        futures.add(client.sendAsync(request, HttpResponse.BodyHandlers.ofString()))

        for (i in 1..MAX_ATTEMPTS) {
            scheduler.schedule({
                if (futures.any { it.isDone }) return@schedule

                request = makeRequest(transactionId, paymentId, amount)
                futures.add(client.sendAsync(request, HttpResponse.BodyHandlers.ofString()))
            }, HEDGE_DELAY * i, TimeUnit.MILLISECONDS)
        }

        CompletableFuture.anyOf(*futures.toTypedArray())
            .thenApply { winner ->
                winner as? HttpResponse<String> ?: throw RuntimeException("No response received")
            }
            .thenApply { response ->
                val body = try {
                    mapper.readValue(response.body(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }
                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                val currentTime = now()
                val result = body.result
                val message = body.message
                dbScope.launch {
                    while (true) {
                        try {
                            paymentESService.update(paymentId) {
                                it.logProcessing(result, currentTime, transactionId, reason = message)
                            }
                            break
                        } catch (_: java.lang.IllegalArgumentException) {
                            delay(10)
                        }
                    }
                }

                ongoingWindow.release()
            }.exceptionally { ex ->
                when (ex) {
                    is SocketTimeoutException -> {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", ex)
                        val currentTime = now()
                        dbScope.launch {
                            while (true) {
                                try {
                                    paymentESService.update(paymentId) {
                                        it.logProcessing(false, currentTime, transactionId, reason = "Request timeout.")
                                    }
                                    break
                                } catch (_: java.lang.IllegalArgumentException) {
                                    delay(10)
                                }
                            }
                        }
                    }
                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", ex)
                        val currentTime = now()
                        dbScope.launch {
                            while (true) {
                                try {
                                    paymentESService.update(paymentId) {
                                        it.logProcessing(false, currentTime, transactionId, reason = ex.message)
                                    }
                                    break
                                } catch (_: java.lang.IllegalArgumentException) {
                                    delay(10)
                                }
                            }
                        }
                    }
                }

                ongoingWindow.release()
            }
    }

    fun makeRequest(transactionId: UUID, paymentId: UUID, amount: Int): HttpRequest{
        return HttpRequest
            .newBuilder()
            .uri(URI("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .timeout(TIMEOUT)
            .header("x-idempotency-key", transactionId.toString())
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    override fun maxRateLimit() = properties.rateLimitPerSec
}

public fun now() = System.currentTimeMillis()