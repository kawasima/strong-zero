package net.unit8.zero;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import static org.junit.jupiter.api.Assertions.*;

class ZeroMessageTest {

    @Test
    void noArgsConstructor_andSetters() {
        ZeroMessage msg = new ZeroMessage();
        msg.setId("abc123");
        msg.setType("USER");
        msg.setMessage(new byte[]{1, 2, 3});

        assertEquals("abc123", msg.getId());
        assertEquals("USER", msg.getType());
        assertArrayEquals(new byte[]{1, 2, 3}, msg.getMessage());
    }

    @Test
    void allArgsConstructor() {
        byte[] payload = {10, 20, 30};
        ZeroMessage msg = new ZeroMessage("id-001", "EVENT", payload);

        assertEquals("id-001", msg.getId());
        assertEquals("EVENT", msg.getType());
        assertArrayEquals(payload, msg.getMessage());
    }

    @Test
    void messagePackRoundTrip() throws Exception {
        ObjectMapper mapper = new ObjectMapper(new MessagePackFactory());

        ZeroMessage original = new ZeroMessage("flake-id-32chars", "ORDER", new byte[]{0x0A, 0x0B, 0x0C});

        byte[] packed = mapper.writeValueAsBytes(original);
        assertNotNull(packed);
        assertTrue(packed.length > 0);

        ZeroMessage deserialized = mapper.readValue(packed, ZeroMessage.class);
        assertEquals(original.getId(), deserialized.getId());
        assertEquals(original.getType(), deserialized.getType());
        assertArrayEquals(original.getMessage(), deserialized.getMessage());
    }

    @Test
    void equalsAndHashCode() {
        ZeroMessage msg1 = new ZeroMessage("id1", "TYPE", new byte[]{1});
        ZeroMessage msg2 = new ZeroMessage("id1", "TYPE", new byte[]{1});
        ZeroMessage msg3 = new ZeroMessage("id2", "TYPE", new byte[]{1});

        assertEquals(msg1, msg2);
        assertEquals(msg1.hashCode(), msg2.hashCode());
        assertNotEquals(msg1, msg3);
    }

    @Test
    void toString_containsFieldValues() {
        ZeroMessage msg = new ZeroMessage("id-test", "INFO", new byte[]{});
        String str = msg.toString();

        assertTrue(str.contains("id-test"));
        assertTrue(str.contains("INFO"));
    }

    @Test
    void nullFields_areHandled() {
        ZeroMessage msg = new ZeroMessage(null, null, null);
        assertNull(msg.getId());
        assertNull(msg.getType());
        assertNull(msg.getMessage());
    }
}
