package net.unit8.zero.consumer;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class StrongZeroConsumerTest {

    private JdbcDataSource dataSource;
    private static final String PRODUCER_ID = "test-producer";
    private static final String PRODUCER_ADDRESS = "tcp://127.0.0.1:19999";
    private static final String DUMMY_MESSAGE_ID = "0000000000000000";

    @BeforeEach
    void setUp() throws Exception {
        dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:consumer_test_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1");

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE consumed_zero ("
                    + "producer_id VARCHAR(16) NOT NULL PRIMARY KEY,"
                    + "last_id VARCHAR(32) NOT NULL)");
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
    void consumed_updatesLastIdInDatabase() throws Exception {
        insertConsumedRow(PRODUCER_ID, DUMMY_MESSAGE_ID);

        StrongZeroConsumer consumer = new StrongZeroConsumer(PRODUCER_ID, PRODUCER_ADDRESS, dataSource);
        initializeLastId(consumer);

        consumer.consumed("abc123");

        assertEquals("abc123", selectLastId(PRODUCER_ID));
    }

    @Test
    void consumed_updatesInternalLastId() throws Exception {
        insertConsumedRow(PRODUCER_ID, DUMMY_MESSAGE_ID);

        StrongZeroConsumer consumer = new StrongZeroConsumer(PRODUCER_ID, PRODUCER_ADDRESS, dataSource);
        initializeLastId(consumer);

        consumer.consumed("xyz789");
        consumer.consumed("xyz999");

        assertEquals("xyz999", selectLastId(PRODUCER_ID));
    }

    @Test
    void consumed_throwsOnDatabaseError() throws Exception {
        JdbcDataSource brokenDs = new JdbcDataSource();
        brokenDs.setURL("jdbc:h2:mem:broken_" + System.nanoTime());
        try (Connection conn = brokenDs.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("SHUTDOWN");
        }

        StrongZeroConsumer consumer = new StrongZeroConsumer(PRODUCER_ID, PRODUCER_ADDRESS, brokenDs);
        initializeLastId(consumer);

        assertThrows(IllegalStateException.class, () -> consumer.consumed("fail-id"));
    }

    @Test
    void getLastId_returnsExistingValue() throws Exception {
        insertConsumedRow(PRODUCER_ID, "existing-id-123");

        StrongZeroConsumer consumer = new StrongZeroConsumer(PRODUCER_ID, PRODUCER_ADDRESS, dataSource);

        java.lang.reflect.Method getLastId = StrongZeroConsumer.class.getDeclaredMethod("getLastId", String.class);
        getLastId.setAccessible(true);
        String result = (String) getLastId.invoke(consumer, PRODUCER_ID);

        assertEquals("existing-id-123", result);
    }

    @Test
    void getLastId_insertsDummyWhenNoRow() throws Exception {
        StrongZeroConsumer consumer = new StrongZeroConsumer(PRODUCER_ID, PRODUCER_ADDRESS, dataSource);

        java.lang.reflect.Method getLastId = StrongZeroConsumer.class.getDeclaredMethod("getLastId", String.class);
        getLastId.setAccessible(true);
        String result = (String) getLastId.invoke(consumer, PRODUCER_ID);

        assertEquals(DUMMY_MESSAGE_ID, result);
        assertEquals(DUMMY_MESSAGE_ID, selectLastId(PRODUCER_ID));
    }

    @Test
    void registerHandler_handlerIsStored() {
        StrongZeroConsumer consumer = new StrongZeroConsumer(PRODUCER_ID, PRODUCER_ADDRESS, dataSource);
        CheckedConsumer handler = (id, message) -> {};
        consumer.registerHandler("USER", handler);

        try {
            java.lang.reflect.Field handlersField = StrongZeroConsumer.class.getDeclaredField("handlers");
            handlersField.setAccessible(true);
            @SuppressWarnings("unchecked")
            java.util.Map<String, CheckedConsumer> handlers =
                    (java.util.Map<String, CheckedConsumer>) handlersField.get(consumer);
            assertSame(handler, handlers.get("USER"));
        } catch (Exception e) {
            fail("Could not access handlers field: " + e.getMessage());
        }
    }

    @Test
    void setAutoAcknowledge_canBeToggled() {
        StrongZeroConsumer consumer = new StrongZeroConsumer(PRODUCER_ID, PRODUCER_ADDRESS, dataSource);

        try {
            java.lang.reflect.Field aaField = StrongZeroConsumer.class.getDeclaredField("autoAcknowledge");
            aaField.setAccessible(true);
            assertTrue((boolean) aaField.get(consumer), "Default should be true");

            consumer.setAutoAcknowledge(false);
            assertFalse((boolean) aaField.get(consumer));

            consumer.setAutoAcknowledge(true);
            assertTrue((boolean) aaField.get(consumer));
        } catch (Exception e) {
            fail("Could not access autoAcknowledge field: " + e.getMessage());
        }
    }

    @Test
    void consumed_commitsTransactionToDatabase() throws Exception {
        insertConsumedRow(PRODUCER_ID, DUMMY_MESSAGE_ID);

        StrongZeroConsumer consumer = new StrongZeroConsumer(PRODUCER_ID, PRODUCER_ADDRESS, dataSource);
        initializeLastId(consumer);

        consumer.consumed("committed-id");

        // Value is visible from a separate connection = commit succeeded
        assertEquals("committed-id", selectLastId(PRODUCER_ID));
    }

    @Test
    void consumed_doesNotUpdateOnFailure() throws Exception {
        insertConsumedRow(PRODUCER_ID, "original-id");

        // Wrap DataSource: connections return PreparedStatements whose executeUpdate always fails
        DataSource failingDs = wrapDataSourceWithFailingUpdate(dataSource);

        StrongZeroConsumer consumer = new StrongZeroConsumer(PRODUCER_ID, PRODUCER_ADDRESS, failingDs);
        initializeLastId(consumer);

        assertThrows(IllegalStateException.class, () -> consumer.consumed("should-not-persist"));

        // Original value preserved = rollback worked
        assertEquals("original-id", selectLastId(PRODUCER_ID));
    }

    // --- helpers ---

    private void insertConsumedRow(String producerId, String lastId) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "INSERT INTO consumed_zero (producer_id, last_id) VALUES (?, ?)")) {
            stmt.setString(1, producerId);
            stmt.setString(2, lastId);
            stmt.executeUpdate();
        }
    }

    private String selectLastId(String producerId) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT last_id FROM consumed_zero WHERE producer_id = ?")) {
            stmt.setString(1, producerId);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next(), "Row should exist for producer: " + producerId);
            return rs.getString("last_id");
        }
    }

    private void initializeLastId(StrongZeroConsumer consumer) throws Exception {
        java.lang.reflect.Field lastIdField = StrongZeroConsumer.class.getDeclaredField("lastId");
        lastIdField.setAccessible(true);
        lastIdField.set(consumer, new AtomicReference<>(DUMMY_MESSAGE_ID));
    }

    /**
     * Creates a DataSource proxy that wraps real connections so that
     * PreparedStatement.executeUpdate() always throws SQLException.
     * Other methods (setAutoCommit, commit, rollback, close, etc.) work normally.
     */
    private static DataSource wrapDataSourceWithFailingUpdate(DataSource real) {
        return (DataSource) Proxy.newProxyInstance(
                DataSource.class.getClassLoader(),
                new Class<?>[]{DataSource.class},
                (proxy, method, args) -> {
                    Object result = method.invoke(real, args);
                    if ("getConnection".equals(method.getName()) && result instanceof Connection conn) {
                        return wrapConnectionWithFailingUpdate(conn);
                    }
                    return result;
                });
    }

    private static Connection wrapConnectionWithFailingUpdate(Connection real) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, args) -> {
                    Object result = method.invoke(real, args);
                    if ("prepareStatement".equals(method.getName()) && result instanceof PreparedStatement stmt) {
                        return wrapPreparedStatementWithFailingUpdate(stmt);
                    }
                    return result;
                });
    }

    private static PreparedStatement wrapPreparedStatementWithFailingUpdate(PreparedStatement real) {
        return (PreparedStatement) Proxy.newProxyInstance(
                PreparedStatement.class.getClassLoader(),
                new Class<?>[]{PreparedStatement.class},
                (InvocationHandler) (proxy, method, args) -> {
                    if ("executeUpdate".equals(method.getName())) {
                        throw new SQLException("Simulated failure");
                    }
                    return method.invoke(real, args);
                });
    }
}
