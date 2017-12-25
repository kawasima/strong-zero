package net.unit8.zero.sender;

import java.io.UncheckedIOException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.time.Clock;
import java.util.Collections;
import java.util.Enumeration;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

public class Flake {
    private Clock clock;
    private int sequence;
    private long lastTime;
    private byte[] macAddress;

    private ReentrantLock lock = new ReentrantLock();
    private static final char[] HEX = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    public Flake() {
        this(Clock.systemUTC());
    }

    public Flake(Clock clock) {
        this.clock = clock;
        sequence = 0;
        lastTime = clock.millis();
        macAddress = getMacAddress();
    }

    private byte[] getMacAddress() {
        //noinspection unchecked
        Enumeration<NetworkInterface> interfaces = (Enumeration<NetworkInterface>) ((Supplier<Object>) () -> {
            try {
                return NetworkInterface.getNetworkInterfaces();
            } catch (SocketException e) {
                throw new UncheckedIOException(e);
            }
        }).get();
        return Collections.list(interfaces)
                .stream()
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
                .findAny()
                .orElseThrow(IllegalStateException::new);
    }

    public String generateId() {
        byte[] content = new byte[16];
        long current = clock.millis();

        lock.lock();
        try {
            if (current != lastTime) {
                lastTime = current;
                sequence = 0;
            } else {
                sequence++;
            }
        } finally {
            lock.unlock();
        }
        content[0] = (byte)((int) (lastTime >> 56));
        content[1] = (byte)((int) (lastTime >> 48));
        content[2] = (byte)((int) (lastTime >> 40));
        content[3] = (byte)((int) (lastTime >> 32));
        content[4] = (byte)((int) (lastTime >> 24));
        content[5] = (byte)((int) (lastTime >> 16));
        content[6] = (byte)((int) (lastTime >> 8));
        content[7] = (byte)((int) (lastTime));
        content[8] = macAddress[0];
        content[9] = macAddress[1];
        content[10] = macAddress[2];
        content[11] = macAddress[3];
        content[12] = macAddress[4];
        content[13] = macAddress[5];
        content[14] = (byte) (sequence >> 8);
        content[15] = (byte) sequence;

        char[] id = new char[]{
                HEX[(content[0] & 240) >> 4], HEX[content[0] & 15],
                HEX[(content[1] & 240) >> 4], HEX[content[1] & 15],
                HEX[(content[2] & 240) >> 4], HEX[content[2] & 15],
                HEX[(content[3] & 240) >> 4], HEX[content[3] & 15],
                HEX[(content[4] & 240) >> 4], HEX[content[4] & 15],
                HEX[(content[5] & 240) >> 4], HEX[content[5] & 15],
                HEX[(content[6] & 240) >> 4], HEX[content[6] & 15],
                HEX[(content[7] & 240) >> 4], HEX[content[7] & 15],
                HEX[(content[8] & 240) >> 4], HEX[content[8] & 15],
                HEX[(content[9] & 240) >> 4], HEX[content[9] & 15],
                HEX[(content[10] & 240) >> 4], HEX[content[10] & 15],
                HEX[(content[11] & 240) >> 4], HEX[content[11] & 15],
                HEX[(content[12] & 240) >> 4], HEX[content[12] & 15],
                HEX[(content[13] & 240) >> 4], HEX[content[13] & 15],
                HEX[(content[14] & 240) >> 4], HEX[content[14] & 15],
                HEX[(content[15] & 240) >> 4], HEX[content[15] & 15]
        };
        return new String(id);
    }
}
