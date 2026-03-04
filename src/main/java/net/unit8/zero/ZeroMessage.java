package net.unit8.zero;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * A message transferred between producer and consumer via the outbox table.
 */
@Data
@AllArgsConstructor
public class ZeroMessage implements Serializable {
    /** Flake-generated unique identifier. */
    private String id;
    /** Message type used for handler dispatch. */
    private String type;
    /** Serialized message payload. */
    private byte[] message;

    /** Creates a new empty ZeroMessage. */
    public ZeroMessage() {}
}
