package net.unit8.zero.consumer;

public interface CheckedConsumer {
    void consume(String id, byte[] message) throws Exception;
}
