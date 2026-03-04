package net.unit8.zero.consumer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.Getter;

/**
 * Micrometer metrics for message consumption.
 */
@Getter
public class ConsumerMetrics {
    private Counter consumeMessages;
    private Timer consumeTime;

    /**
     * Creates metrics and registers them with the given registry.
     *
     * @param registry the meter registry to register metrics with
     */
    public ConsumerMetrics(MeterRegistry registry) {
        consumeMessages = registry.counter("strong_zero.consumer.messages");
        consumeTime     = registry.timer("strong_zero.consumer.time");
    }
}
