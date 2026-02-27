package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.ktor.client.*
import io.ktor.client.engine.java.Java
import io.ktor.client.request.*
import io.ktor.client.statement.*
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Semaphore
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NonBlockingSlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val dbScope: CoroutineScope
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val REQUEST_TIMEOUT = 1000L
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val semaphore = Semaphore(permits = parallelRequests)

    private val client = HttpClient(Java) {
        engine {
            protocolVersion = java.net.http.HttpClient.Version.HTTP_2
        }
        expectSuccess = false
        install(io.ktor.client.plugins.HttpTimeout) {
            requestTimeoutMillis = REQUEST_TIMEOUT
        }
    }

    private val rateLimiter: NonBlockingSlidingWindowRateLimiter by lazy {
        NonBlockingSlidingWindowRateLimiter(rate = rateLimitPerSec)
    }

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
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
                } catch (_: IllegalArgumentException) {
                    delay(10)
                }
            }
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        val result = send(paymentId, amount, transactionId, paymentStartedAt)
        val processedAt = now()

        dbScope.launch {
            while (true) {
                try {
                    paymentESService.update(paymentId) {
                        it.logProcessing(result.status, processedAt, transactionId, reason = result.message)
                    }
                    break
                } catch (_: IllegalArgumentException) {
                    delay(10)
                }
            }
        }
    }

    private suspend fun send(paymentId: UUID, amount: Int, transactionId: UUID, paymentStartedAt: Long): Result {
        try {
            semaphore.acquire()
            if (!rateLimiter.acquireSuspend(200L)) {
                return Result(false, "Rate limit exceeded")
            }

            val response = client.post("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")

            val body = try {
                mapper.readValue(response.bodyAsText(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.status.value}, reason: ${response.bodyAsText()}")
                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
            }

            logger.info("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}.")
            return Result(true, body.message)
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId.", e)
                }
                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId.", e)
                }
            }
            return Result(false, e.message)
        } finally {
            semaphore.release()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    data class Result(val status: Boolean, val message: String?)
}

fun now() = System.currentTimeMillis()