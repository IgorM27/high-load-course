package ru.quipy.payments.logic

import java.time.Duration

class QuantileBasedTimeoutCalculator(
    private val targetQuantile: Double = 0.90,
    private val quantile50Margin: Double = 0.20,
    private val minTimeout: Duration = Duration.ofSeconds(1),
    private val maxTimeout: Duration = Duration.ofSeconds(5)
) {
    private val latencyMeasurements = mutableListOf<Long>()

    fun addLatencyMeasurement(latencyMs: Long) {
        synchronized(latencyMeasurements) {
            latencyMeasurements.add(latencyMs)
        }
    }

    fun calculateOptimalTimeout(): Duration {
        synchronized(latencyMeasurements) {
            if (latencyMeasurements.isEmpty()) {
                return Duration.ofMillis((1200 * 1.5).toLong())
            }

            val sortedLatencies = latencyMeasurements.sorted()
            val n = sortedLatencies.size

            // Вычисляем медиану (50-й перцентиль)
            val median = calculateQuantile(sortedLatencies, n, 0.5)

            // Вычисляем целевой перцентиль (90-й)
            val targetQuantileValue = calculateQuantile(sortedLatencies, n, targetQuantile)

            // Применяем ограничение: target quantile не должен превышать median более чем на 20%
            val maxAllowedByMedian = median * (1 + quantile50Margin)
            val optimalTimeout = minOf(targetQuantileValue, maxAllowedByMedian)

            // Применяем границы
            val boundedTimeout = optimalTimeout.coerceIn(
                minTimeout.toMillis().toDouble(),
                maxTimeout.toMillis().toDouble()
            )

            return Duration.ofMillis(boundedTimeout.toLong())
        }
    }

    private fun calculateQuantile(sortedData: List<Long>, n: Int, quantile: Double): Double {
        val pos = quantile * (n - 1)
        val lowerIndex = pos.toInt()
        val fraction = pos - lowerIndex

        return if (lowerIndex >= n - 1) {
            sortedData.last().toDouble()
        } else {
            sortedData[lowerIndex] * (1 - fraction) + sortedData[lowerIndex + 1] * fraction
        }
    }
}