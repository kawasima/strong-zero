package net.unit8.zero.producer;

import io.micrometer.core.instrument.MeterRegistry;

import java.util.Deque;

/**
 * Micrometer metrics for the producer's pump worker queue.
 */
public class ProducerMetrics {
    /**
     * Creates metrics and registers a gauge for the worker queue size.
     *
     * @param registry    the meter registry to register metrics with
     * @param workerQueue the pump worker queue to monitor
     */
    public ProducerMetrics(MeterRegistry registry, Deque<?> workerQueue) {
        registry.gauge("strong_zero.pump.count", workerQueue, Deque::size);
    }
}
