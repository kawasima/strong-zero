package net.unit8.zero.consumer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.Getter;

@Getter
public class ConsumerMetrics {
    private Counter consumeMessages;
    private Timer consumeTime;
    public ConsumerMetrics(MeterRegistry registry) {
        consumeMessages = registry.counter("consume.messages");
        consumeTime     = registry.timer("consume.time");
    }
}
