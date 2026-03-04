package net.unit8.zero.sender;

import java.io.UncheckedIOException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.time.Clock;
import java.util.HexFormat;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread-safe unique ID generator based on timestamp, MAC address, and sequence number.
 * Generates 128-bit (32 hex character) identifiers that are time-ordered and unique across hosts.
 *
 * <p>ID layout (16 bytes):
 * <ul>
 *   <li>Bytes 0-7: timestamp in milliseconds (big-endian)</li>
 *   <li>Bytes 8-13: MAC address</li>
 *   <li>Bytes 14-15: sequence number within the same millisecond</li>
 * </ul>
 */
public class Flake {
    private static final int MAX_SEQUENCE = 0xFFFF;
    private static final HexFormat HEX = HexFormat.of();

    private final Clock clock;
    private final byte[] macAddress;
    private final ReentrantLock lock = new ReentrantLock();

    private int sequence;
    private long lastTime;

    /** Creates a Flake using the system UTC clock. */
    public Flake() {
        this(Clock.systemUTC());
    }

    /**
     * Creates a Flake with the given clock.
     *
     * @param clock the clock to use for timestamp generation
     */
    public Flake(Clock clock) {
        this.clock = clock;
        this.sequence = 0;
        this.lastTime = clock.millis();
        this.macAddress = getMacAddress();
    }

    private byte[] getMacAddress() {
        try {
            return NetworkInterface.networkInterfaces()
                    .filter(ni -> {
                        try {
                            return !ni.isLoopback() && ni.isUp();
                        } catch (SocketException e) {
                            throw new UncheckedIOException(e);
                        }
                    })
                    .map(ni -> {
                        try {
                            return ni.getHardwareAddress();
                        } catch (SocketException e) {
                            throw new UncheckedIOException(e);
                        }
                    })
                    .filter(Objects::nonNull)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("No network interface with hardware address found"));
        } catch (SocketException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Generates a unique 32-character hex ID.
     *
     * @return the generated identifier
     */
    public String generateId() {
        byte[] content = new byte[16];
        long time;
        int seq;

        lock.lock();
        try {
            long current = clock.millis();
            if (current != lastTime) {
                lastTime = current;
                sequence = 0;
            } else {
                if (sequence >= MAX_SEQUENCE) {
                    while (clock.millis() == lastTime) {
                        Thread.onSpinWait();
                    }
                    lastTime = clock.millis();
                    sequence = 0;
                } else {
                    sequence++;
                }
            }
            time = lastTime;
            seq = sequence;
        } finally {
            lock.unlock();
        }

        content[0] = (byte) (time >> 56);
        content[1] = (byte) (time >> 48);
        content[2] = (byte) (time >> 40);
        content[3] = (byte) (time >> 32);
        content[4] = (byte) (time >> 24);
        content[5] = (byte) (time >> 16);
        content[6] = (byte) (time >> 8);
        content[7] = (byte) time;
        System.arraycopy(macAddress, 0, content, 8, 6);
        content[14] = (byte) (seq >> 8);
        content[15] = (byte) seq;

        return HEX.formatHex(content);
    }
}
