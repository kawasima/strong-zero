package net.unit8.zero.producer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Deque;

import static org.junit.jupiter.api.Assertions.*;

class ProducerMetricsTest {

    @Test
    void gauge_reflectsQueueSize() {
        MeterRegistry registry = new SimpleMeterRegistry();
        Deque<String> queue = new ArrayDeque<>();

        new ProducerMetrics(registry, queue);

        Gauge gauge = registry.find("strong_zero.pump.count").gauge();
        assertNotNull(gauge, "Gauge should be registered as strong_zero.pump.count");

        assertEquals(0, gauge.value());

        queue.add("item1");
        assertEquals(1, gauge.value());

        queue.add("item2");
        queue.add("item3");
        assertEquals(3, gauge.value());

        queue.poll();
        assertEquals(2, gauge.value());
    }
}
