package net.unit8.zero.sender;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class StrongZeroSenderTest {

    private JdbcDataSource dataSource;
    private ObjectMapper mapper;
    private ZContext testCtx;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:sender_test_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1");
        mapper = new ObjectMapper(new MessagePackFactory());

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE produced_zero ("
                    + "id VARCHAR(32) NOT NULL PRIMARY KEY,"
                    + "type VARCHAR(255) NOT NULL,"
                    + "message BLOB)");
        }

        testCtx = new ZContext();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (testCtx != null && !testCtx.isClosed()) {
            testCtx.close();
        }
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP ALL OBJECTS");
        }
    }

    @Test
    void send_insertsMessageIntoDatabase() throws Exception {
        String addr = "inproc://sender-test-insert-" + System.nanoTime();
        try (StrongZeroSender sender = new StrongZeroSender(addr, dataSource, mapper)) {
            sender.send("USER", new TestPayload("alice", 30));

            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement("SELECT id, type, message FROM produced_zero")) {
                ResultSet rs = stmt.executeQuery();
                assertTrue(rs.next(), "Should have one row");
                String id = rs.getString("id");
                String type = rs.getString("type");
                byte[] message = rs.getBytes("message");

                assertEquals("USER", type);
                assertNotNull(id);
                assertEquals(32, id.length());
                assertTrue(id.matches("[0-9a-f]{32}"), "ID should be hex: " + id);
                assertNotNull(message);
                assertTrue(message.length > 0);

                assertFalse(rs.next(), "Should have exactly one row");
            }
        }
    }

    @Test
    void send_multipleInserts_haveUniqueIds() throws Exception {
        String addr = "inproc://sender-test-multi-" + System.nanoTime();
        try (StrongZeroSender sender = new StrongZeroSender(addr, dataSource, mapper)) {
            for (int i = 0; i < 5; i++) {
                sender.send("EVENT", new TestPayload("user" + i, i));
            }

            List<String> ids = new ArrayList<>();
            try (Connection conn = dataSource.getConnection();
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT id FROM produced_zero ORDER BY id")) {
                while (rs.next()) {
                    ids.add(rs.getString("id"));
                }
            }

            assertEquals(5, ids.size());
            assertEquals(ids.size(), ids.stream().distinct().count(), "All IDs should be unique");

            // Verify ordering
            for (int i = 1; i < ids.size(); i++) {
                assertTrue(ids.get(i).compareTo(ids.get(i - 1)) > 0,
                        "IDs should be time-ordered");
            }
        }
    }

    @Test
    void send_triggersNotificationAfterThreshold() throws Exception {
        int port = 15000 + (int) (System.nanoTime() % 5000);
        String addr = "tcp://127.0.0.1:" + port;

        try (StrongZeroSender sender = new StrongZeroSender(addr, dataSource, mapper)) {
            ZMQ.Socket sub = testCtx.createSocket(SocketType.SUB);
            sub.subscribe(ZMQ.SUBSCRIPTION_ALL);
            sub.setReceiveTimeOut(5000);
            sub.connect(addr);
            // Allow subscription to propagate
            Thread.sleep(300);

            // Default threshold is 3. Send 2 messages - no notification expected
            sender.send("T", new TestPayload("a", 1));
            sender.send("T", new TestPayload("b", 2));

            String earlyMsg = sub.recvStr(ZMQ.DONTWAIT);
            assertNull(earlyMsg, "Should not receive notification before threshold");

            // 3rd message should trigger notification
            sender.send("T", new TestPayload("c", 3));

            // Wait for notification
            String notifyMsg = sub.recvStr();
            assertNotNull(notifyMsg, "Should receive notification after threshold");
            assertEquals("update", notifyMsg);
        }
    }

    @Test
    void send_throwsOnSerializationError() throws Exception {
        String addr = "inproc://sender-test-sererr-" + System.nanoTime();
        // Use a mapper that cannot serialize the object
        ObjectMapper brokenMapper = new ObjectMapper(new MessagePackFactory());

        try (StrongZeroSender sender = new StrongZeroSender(addr, dataSource, brokenMapper)) {
            // Object that causes serialization failure
            Object unserializable = new Object() {
                // Self-referencing causes infinite recursion
                @SuppressWarnings("unused")
                public Object getSelf() { return this; }
            };

            assertThrows(IllegalStateException.class, () -> sender.send("BAD", unserializable));
        }
    }

    @Test
    void close_closesZContext() throws Exception {
        String addr = "inproc://sender-test-close-" + System.nanoTime();
        StrongZeroSender sender = new StrongZeroSender(addr, dataSource, mapper);
        sender.close();
        // Calling close again should not throw
        sender.close();
    }

    // Simple POJO for test serialization
    public static class TestPayload {
        private String name;
        private int age;

        public TestPayload() {}

        public TestPayload(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public int getAge() { return age; }
        public void setAge(int age) { this.age = age; }
    }
}
