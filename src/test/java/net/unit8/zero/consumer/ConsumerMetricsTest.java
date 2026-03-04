package net.unit8.zero.consumer;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class ConsumerMetricsTest {

    private MeterRegistry registry;
    private ConsumerMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new ConsumerMetrics(registry);
    }

    @Test
    void counterIncrements() {
        assertEquals(0, metrics.getConsumeMessages().count());

        metrics.getConsumeMessages().increment();
        assertEquals(1, metrics.getConsumeMessages().count());

        metrics.getConsumeMessages().increment();
        metrics.getConsumeMessages().increment();
        assertEquals(3, metrics.getConsumeMessages().count());
    }

    @Test
    void timerRecords() {
        assertEquals(0, metrics.getConsumeTime().count());

        metrics.getConsumeTime().record(Duration.ofMillis(100));
        assertEquals(1, metrics.getConsumeTime().count());

        metrics.getConsumeTime().record(Duration.ofMillis(200));
        assertEquals(2, metrics.getConsumeTime().count());
    }

    @Test
    void metricNames_haveCorrectPrefix() {
        assertNotNull(registry.find("strong_zero.consumer.messages").counter(),
                "Counter should be registered as strong_zero.consumer.messages");

        assertNotNull(registry.find("strong_zero.consumer.time").timer(),
                "Timer should be registered as strong_zero.consumer.time");
    }
}
