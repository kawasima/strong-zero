package net.unit8.zero.pump;

import net.unit8.zero.ZeroMessage;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class StrongZeroPumpTest {

    private JdbcDataSource dataSource;
    private static final String BACKEND_ADDRESS = "inproc://pump-test-backend";
    private static final String NOTIFICATION_ADDRESS = "inproc://pump-test-notify";

    @BeforeEach
    void setUp() throws Exception {
        dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:pump_test_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1");

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE produced_zero ("
                    + "id VARCHAR(32) NOT NULL PRIMARY KEY,"
                    + "type VARCHAR(255) NOT NULL,"
                    + "message BLOB)");
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP ALL OBJECTS");
        }
    }

    @Test
    void fetchMessages_returnsAllMessages() throws Exception {
        insertMessage("00000001", "USER", "payload1".getBytes());
        insertMessage("00000002", "USER", "payload2".getBytes());
        insertMessage("00000003", "EVENT", "payload3".getBytes());

        StrongZeroPump pump = createPump(dataSource);
        List<ZeroMessage> messages = invokeFetchMessages(pump, "00000000");

        assertEquals(3, messages.size());
        assertEquals("00000001", messages.get(0).getId());
        assertEquals("USER", messages.get(0).getType());
        assertEquals("00000002", messages.get(1).getId());
        assertEquals("00000003", messages.get(2).getId());
        assertEquals("EVENT", messages.get(2).getType());

        pump.close();
    }

    @Test
    void fetchMessages_filtersById() throws Exception {
        insertMessage("00000001", "A", "a".getBytes());
        insertMessage("00000002", "B", "b".getBytes());
        insertMessage("00000003", "C", "c".getBytes());

        StrongZeroPump pump = createPump(dataSource);
        List<ZeroMessage> messages = invokeFetchMessages(pump, "00000002");

        assertEquals(1, messages.size());
        assertEquals("00000003", messages.get(0).getId());

        pump.close();
    }

    @Test
    void fetchMessages_respectsBatchSize() throws Exception {
        for (int i = 1; i <= 10; i++) {
            insertMessage(String.format("%08d", i), "T", ("msg" + i).getBytes());
        }

        StrongZeroPump pump = createPump(dataSource);
        pump.setBatchSize(3);
        List<ZeroMessage> messages = invokeFetchMessages(pump, "00000000");

        assertEquals(3, messages.size());
        assertEquals("00000001", messages.get(0).getId());
        assertEquals("00000003", messages.get(2).getId());

        pump.close();
    }

    @Test
    void fetchMessages_returnsEmptyListWhenNoMessages() throws Exception {
        StrongZeroPump pump = createPump(dataSource);
        List<ZeroMessage> messages = invokeFetchMessages(pump, "00000000");

        assertNotNull(messages);
        assertTrue(messages.isEmpty());

        pump.close();
    }

    @Test
    void fetchMessages_retriesOnSqlException() throws Exception {
        insertMessage("00000001", "RETRY", "retryPayload".getBytes());

        // DataSource proxy: first getConnection() throws, second returns real connection
        AtomicInteger callCount = new AtomicInteger(0);
        DataSource retryDs = (DataSource) Proxy.newProxyInstance(
                DataSource.class.getClassLoader(),
                new Class<?>[]{DataSource.class},
                (proxy, method, args) -> {
                    if ("getConnection".equals(method.getName())) {
                        if (callCount.getAndIncrement() == 0) {
                            throw new SQLException("Connection lost");
                        }
                    }
                    return method.invoke(dataSource, args);
                });

        StrongZeroPump pump = createPump(retryDs);
        List<ZeroMessage> messages = invokeFetchMessages(pump, "00000000");

        assertEquals(1, messages.size());
        assertEquals("00000001", messages.get(0).getId());
        assertEquals(2, callCount.get(), "getConnection should have been called twice (1 failure + 1 success)");

        pump.close();
    }

    @Test
    void fetchMessages_messageContainsCorrectPayload() throws Exception {
        byte[] payload = {0x01, 0x02, 0x03, 0x04};
        insertMessage("00000001", "BINARY", payload);

        StrongZeroPump pump = createPump(dataSource);
        List<ZeroMessage> messages = invokeFetchMessages(pump, "00000000");

        assertEquals(1, messages.size());
        assertArrayEquals(payload, messages.get(0).getMessage());

        pump.close();
    }

    private void insertMessage(String id, String type, byte[] message) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "INSERT INTO produced_zero (id, type, message) VALUES (?, ?, ?)")) {
            stmt.setString(1, id);
            stmt.setString(2, type);
            stmt.setBytes(3, message);
            stmt.executeUpdate();
        }
    }

    private StrongZeroPump createPump(DataSource ds) {
        String suffix = String.valueOf(System.nanoTime());
        return new StrongZeroPump(
                BACKEND_ADDRESS + "-" + suffix,
                NOTIFICATION_ADDRESS + "-" + suffix,
                ds
        );
    }

    @SuppressWarnings("unchecked")
    private List<ZeroMessage> invokeFetchMessages(StrongZeroPump pump, String lastId) throws Exception {
        Method fetchMessages = StrongZeroPump.class.getDeclaredMethod("fetchMessages", String.class);
        fetchMessages.setAccessible(true);
        return (List<ZeroMessage>) fetchMessages.invoke(pump, lastId);
    }
}
