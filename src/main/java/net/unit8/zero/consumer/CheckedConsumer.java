package net.unit8.zero.consumer;

/**
 * A message handler that may throw checked exceptions.
 */
@FunctionalInterface
public interface CheckedConsumer {
    /**
     * Consumes a message.
     *
     * @param id      the message identifier
     * @param message the serialized message payload
     * @throws Exception if message processing fails
     */
    void consume(String id, byte[] message) throws Exception;
}
