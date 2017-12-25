package net.unit8.zero;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class ZeroMessage implements Serializable {
    private String id;
    private String type;
    private byte[] message;
}
