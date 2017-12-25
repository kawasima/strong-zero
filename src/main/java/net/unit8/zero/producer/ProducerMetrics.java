package net.unit8.zero.producer;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.Getter;

@Getter
public class ProducerMetrics {
    public ProducerMetrics(MeterRegistry registry) {
        registry.gauge("pump.count", 0);
        registry.gauge("consumer.count", 0);
    }
}
