package net.unit8.zero;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.histogram.HistogramConfig;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;

public class ZeroMeterRegistry extends MeterRegistry {
    public ZeroMeterRegistry() {
        this(Clock.SYSTEM);
    }

    protected ZeroMeterRegistry(Clock clock) {
        super(clock);
    }

    @Override
    protected <T> Gauge newGauge(Meter.Id id, T t, ToDoubleFunction<T> toDoubleFunction) {
        return new Gauge() {
            @Override
            public double value() {
                return 0;
            }

            @Override
            public Id getId() {
                return id;
            }
        };
    }

    @Override
    protected Counter newCounter(Meter.Id id) {
        return new Counter() {
            @Override
            public void increment(double v) {

            }

            @Override
            public double count() {
                return 0;
            }

            @Override
            public Id getId() {
                return id;
            }
        };
    }

    @Override
    protected LongTaskTimer newLongTaskTimer(Meter.Id id) {
        return new LongTaskTimer() {
            @Override
            public long start() {
                return 0;
            }

            @Override
            public long stop(long l) {
                return 0;
            }

            @Override
            public double duration(long l, TimeUnit timeUnit) {
                return 0;
            }

            @Override
            public double duration(TimeUnit timeUnit) {
                return 0;
            }

            @Override
            public int activeTasks() {
                return 0;
            }

            @Override
            public Id getId() {
                return id;
            }
        };
    }

    @Override
    protected Timer newTimer(Meter.Id id, HistogramConfig histogramConfig) {
        return new Timer() {
            @Override
            public void record(long l, TimeUnit timeUnit) {

            }

            @Override
            public <T> T record(Supplier<T> supplier) {
                return null;
            }

            @Override
            public <T> T recordCallable(Callable<T> callable) throws Exception {
                return null;
            }

            @Override
            public void record(Runnable runnable) {

            }

            @Override
            public long count() {
                return 0;
            }

            @Override
            public double totalTime(TimeUnit timeUnit) {
                return 0;
            }

            @Override
            public double max(TimeUnit timeUnit) {
                return 0;
            }

            @Override
            public double percentile(double v, TimeUnit timeUnit) {
                return 0;
            }

            @Override
            public double histogramCountAtValue(long l) {
                return 0;
            }

            @Override
            public HistogramSnapshot takeSnapshot(boolean b) {
                return null;
            }

            @Override
            public Id getId() {
                return id;
            }
        };
    }

    @Override
    protected DistributionSummary newDistributionSummary(Meter.Id id, HistogramConfig histogramConfig) {
        return new DistributionSummary() {
            @Override
            public void record(double v) {

            }

            @Override
            public long count() {
                return 0;
            }

            @Override
            public double totalAmount() {
                return 0;
            }

            @Override
            public double max() {
                return 0;
            }

            @Override
            public double percentile(double v) {
                return 0;
            }

            @Override
            public double histogramCountAtValue(long l) {
                return 0;
            }

            @Override
            public HistogramSnapshot takeSnapshot(boolean b) {
                return null;
            }

            @Override
            public Id getId() {
                return id;
            }
        };
    }

    @Override
    protected void newMeter(Meter.Id id, Meter.Type type, Iterable<Measurement> iterable) {

    }

    @Override
    protected TimeUnit getBaseTimeUnit() {
        return TimeUnit.MILLISECONDS;
    }
}
