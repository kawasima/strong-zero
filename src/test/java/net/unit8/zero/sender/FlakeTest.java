package net.unit8.zero.sender;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class FlakeTest {

    @Test
    void generateId_returns32CharHexString() {
        Flake flake = new Flake();
        String id = flake.generateId();

        assertEquals(32, id.length());
        assertTrue(id.matches("[0-9a-f]{32}"), "ID should be lowercase hex: " + id);
    }

    @Test
    void generateId_isUniqueOver1000Calls() {
        Flake flake = new Flake();
        Set<String> ids = new HashSet<>();

        for (int i = 0; i < 1000; i++) {
            assertTrue(ids.add(flake.generateId()), "Duplicate ID detected at iteration " + i);
        }
        assertEquals(1000, ids.size());
    }

    @Test
    void generateId_idsAreTimeOrdered() {
        Flake flake = new Flake();
        String prev = flake.generateId();

        for (int i = 0; i < 100; i++) {
            String current = flake.generateId();
            assertTrue(current.compareTo(prev) > 0,
                    "IDs should be monotonically increasing: prev=" + prev + " current=" + current);
            prev = current;
        }
    }

    @Test
    void generateId_sequenceIncrementsWithinSameMillisecond() {
        Instant fixed = Instant.parse("2025-01-01T00:00:00Z");
        Clock clock = Clock.fixed(fixed, ZoneOffset.UTC);
        Flake flake = new Flake(clock);

        String first = flake.generateId();
        String second = flake.generateId();
        String third = flake.generateId();

        // Same timestamp prefix (first 16 hex chars = 8 bytes timestamp)
        assertEquals(first.substring(0, 16), second.substring(0, 16));
        assertEquals(first.substring(0, 16), third.substring(0, 16));

        // Same MAC address (chars 16-27 = 6 bytes MAC)
        assertEquals(first.substring(16, 28), second.substring(16, 28));

        // Sequence part (last 4 hex chars = 2 bytes) should increment
        int seq1 = Integer.parseInt(first.substring(28), 16);
        int seq2 = Integer.parseInt(second.substring(28), 16);
        int seq3 = Integer.parseInt(third.substring(28), 16);

        // First call within same millis gets sequence 0 or incremented from constructor init
        assertEquals(seq1 + 1, seq2);
        assertEquals(seq2 + 1, seq3);
    }

    @Test
    void generateId_sequenceResetsOnTimeChange() {
        AtomicInteger callCount = new AtomicInteger(0);
        // First few calls return time T1, then switch to T2
        Clock clock = new Clock() {
            final Instant t1 = Instant.parse("2025-01-01T00:00:00.000Z");
            final Instant t2 = Instant.parse("2025-01-01T00:00:00.001Z");

            @Override
            public ZoneOffset getZone() {
                return ZoneOffset.UTC;
            }

            @Override
            public Clock withZone(java.time.ZoneId zone) {
                return this;
            }

            @Override
            public Instant instant() {
                return callCount.get() < 5 ? t1 : t2;
            }
        };

        Flake flake = new Flake(clock);
        // Generate a few at t1 to build up sequence
        for (int i = 0; i < 3; i++) {
            flake.generateId();
            callCount.incrementAndGet();
        }

        // Switch to t2
        callCount.set(5);
        String afterTimeChange = flake.generateId();

        // Sequence should be reset to 0 at the new timestamp
        int seq = Integer.parseInt(afterTimeChange.substring(28), 16);
        assertEquals(0, seq, "Sequence should reset to 0 when time changes");
    }

    @Test
    void generateId_isThreadSafe() throws Exception {
        Flake flake = new Flake();
        int threadCount = 10;
        int idsPerThread = 1000;
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        Set<String> allIds = ConcurrentHashMap.newKeySet();

        try {
            var futures = new java.util.ArrayList<Future<?>>();
            for (int t = 0; t < threadCount; t++) {
                futures.add(executor.submit(() -> {
                    try {
                        barrier.await();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    for (int i = 0; i < idsPerThread; i++) {
                        String id = flake.generateId();
                        assertTrue(allIds.add(id), "Duplicate ID in concurrent generation: " + id);
                    }
                }));
            }
            for (var f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdown();
        }

        assertEquals(threadCount * idsPerThread, allIds.size());
    }
}
