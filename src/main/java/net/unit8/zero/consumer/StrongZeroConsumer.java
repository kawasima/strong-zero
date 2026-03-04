package net.unit8.zero.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import net.unit8.zero.ZeroMessage;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Consumes messages from a {@link net.unit8.zero.producer.StrongZeroProducer} via ZeroMQ,
 * tracking the last consumed position in a database table.
 */
public class StrongZeroConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(StrongZeroConsumer.class);
    private static final String UPDATE_SQL = "UPDATE %s SET last_id=? WHERE producer_id=?";
    private static final String INSERT_SQL = "INSERT INTO %s (producer_id,last_id) VALUES(?,?)";
    private static final String SELECT_SQL = "SELECT last_id FROM %s WHERE producer_id = ?";
    private static final String DEFAULT_TABLE_NAME = "consumed_zero";
    private static final String DUMMY_MESSAGE_ID = "0000000000000000";

    private String updateSql;
    private String insertSql;
    private String selectSql;

    private AtomicBoolean running = new AtomicBoolean(false);
    private volatile ExecutorService consumerExecutor;
    private DataSource dataSource;
    private String producerId;
    private String producerAddress;
    private Clock clock = Clock.systemUTC();

    private ObjectMapper mapper;
    private ConsumerMetrics consumerMetrics = new ConsumerMetrics(new SimpleMeterRegistry());
    private AtomicReference<String> lastId;
    private Map<String, CheckedConsumer> handlers;
    private boolean autoAcknowledge = true;
    private RetryPolicy<Object> retryPolicy;
    private RetryPolicy<ZMsg> connectRetryPolicy;

    /**
     * Creates a consumer that connects to the specified producer.
     *
     * @param producerId      identifier of the producer to consume from
     * @param producerAddress ZeroMQ address of the producer
     * @param dataSource      data source for tracking consumed position
     */
    public StrongZeroConsumer(String producerId, String producerAddress, DataSource dataSource) {
        this.producerId = producerId;
        this.producerAddress = producerAddress;
        this.dataSource = dataSource;

        handlers = new HashMap<>();
        retryPolicy = RetryPolicy.builder()
                .handleResultIf(ret -> running.get())
                .build();
        connectRetryPolicy = RetryPolicy.<ZMsg>builder()
                .withDelay(Duration.ofSeconds(10))
                .abortOn(ZMQException.class)
                .handleResultIf(ret -> Objects.equals(ret.getFirst().getString(ZMQ.CHARSET), "BUSY"))
                .build();

        insertSql = String.format(Locale.US, INSERT_SQL, DEFAULT_TABLE_NAME);
        updateSql = String.format(Locale.US, UPDATE_SQL, DEFAULT_TABLE_NAME);
        selectSql = String.format(Locale.US, SELECT_SQL, DEFAULT_TABLE_NAME);
    }

    /**
     * Registers a handler for messages of the given type.
     *
     * @param type     the message type to handle
     * @param consumer the handler to invoke for matching messages
     */
    public void registerHandler(String type, CheckedConsumer consumer) {
        handlers.put(type, consumer);
    }

    private static final int RECV_TIMEOUT_MS = 30_000;

    private Object consumeLoop() throws Exception {
        ZContext ctx = new ZContext();
        try {
            ZMQ.Socket socket = ctx.createSocket(SocketType.REQ);
            socket.setReceiveTimeOut(RECV_TIMEOUT_MS);
            socket.connect(producerAddress);
            socket.monitor("inproc://socket.monitor", ZMQ.EVENT_DISCONNECTED);
            ExecutorService executors = Executors.newFixedThreadPool(2);
            consumerExecutor = executors;

            Future<?> future = executors.submit(() -> {
                while (!ctx.isClosed() && running.get()) {
                    try {
                        ZMsg msg = Failsafe.with(connectRetryPolicy)
                                .get(() -> {
                                    ZMsg request = new ZMsg();
                                    request.push(new ZFrame(lastId.get()));
                                    request.send(socket);
                                    ZMsg reply = ZMsg.recvMsg(socket);
                                    if (reply == null) {
                                        throw new ZMQException("Receive timed out", ZMQ.Error.EAGAIN.getCode());
                                    }
                                    return reply;
                                });
                        String control = msg.popString();
                        if (Objects.equals(control, "SUCCESS")) {
                            List<ZeroMessage> zeroMessages = mapper.readValue(msg.pop().getData(), new TypeReference<List<ZeroMessage>>() {
                            });
                            for (ZeroMessage zeroMessage : zeroMessages) {
                                CheckedConsumer handler = handlers.get(zeroMessage.getType());
                                if (handler == null) {
                                    LOG.warn("No handler registered for type: {}", zeroMessage.getType());
                                    if (autoAcknowledge) {
                                        consumed(zeroMessage.getId());
                                    }
                                    continue;
                                }

                                long start = clock.millis();
                                handler.consume(zeroMessage.getId(), zeroMessage.getMessage());
                                if (autoAcknowledge) {
                                    consumed(zeroMessage.getId());
                                }
                                consumerMetrics.getConsumeMessages().increment();
                                consumerMetrics.getConsumeTime().record(clock.millis() - start, TimeUnit.MILLISECONDS);
                            }
                        } else if (Objects.equals(control, "ERROR")) {
                            String errorMsg = msg.popString();
                            LOG.warn("Received error from producer {}: {}", producerId, errorMsg);
                        }
                    } catch (ZMQException e) {
                        LOG.warn("Producer {} looks like down.", producerId, e);
                    } catch (Exception e) {
                        LOG.warn("Exception occurs when consuming from {}. Retry it.", producerId, e);
                    }
                }
            });
            ZMQ.Socket monitorSocket = ctx.createSocket(SocketType.PAIR);
            monitorSocket.connect("inproc://socket.monitor");
            executors.submit(() -> {
                ZEvent event = ZEvent.recv(monitorSocket);
                LOG.info("SOCKET EVENT={} VALUE={}", event.getEvent(), event.getValue());
                ctx.destroy();
            });
            executors.shutdown();
            return future.get();
        } finally {
            if (!ctx.isClosed()) {
                ctx.close();
            }
            if (consumerExecutor != null) {
                consumerExecutor.shutdownNow();
            }
        }
    }

    /** Starts consuming messages from the producer. Blocks until stopped. */
    public void start() {
        if (mapper == null) mapper = new ObjectMapper(new MessagePackFactory());

        lastId = new AtomicReference<>(getLastId(producerId));
        running.set(true);
        Failsafe.with(retryPolicy)
                .get(this::consumeLoop);
    }

    private void setupDefaultValue(Connection connection) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(insertSql)) {
            stmt.setString(1, producerId);
            stmt.setString(2, DUMMY_MESSAGE_ID);
            stmt.executeUpdate();
            connection.commit();
        }  catch (SQLException e) {
            connection.rollback();
            throw e;
        }
    }
    private String getLastId(String producerId) {
        try(Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement stmt = connection.prepareStatement(selectSql)) {
                stmt.setString(1, producerId);
                try(ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        return rs.getString(1);
                    } else {
                        setupDefaultValue(connection);
                        return DUMMY_MESSAGE_ID;
                    }
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Can't start consumer", e);
        }
    }

    /**
     * Records that a message has been successfully consumed.
     * Updates the last consumed position in the database.
     *
     * @param id the identifier of the consumed message
     */
    public void consumed(String id) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement stmt = connection.prepareStatement(updateSql)) {
                stmt.setString(1, id);
                stmt.setString(2, producerId);
                stmt.executeUpdate();
                connection.commit();
                lastId.set(id);
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to update consumed position for producer: " + producerId, e);
        }
    }

    /** Stops the consumer and shuts down internal executors. */
    public void stop() {
        running.compareAndSet(true, false);
        if (consumerExecutor != null) {
            consumerExecutor.shutdownNow();
        }
    }

    /**
     * Sets the ObjectMapper used for deserializing messages.
     *
     * @param mapper the object mapper to use
     */
    public void setObjectMapper(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * Sets whether messages are automatically acknowledged after handler success.
     * When {@code true} (the default), {@link #consumed(String)} is called automatically
     * after each handler completes without exception. When {@code false}, the caller
     * must invoke {@link #consumed(String)} manually (e.g. within the same transaction).
     *
     * @param autoAcknowledge {@code true} for automatic acknowledgement
     */
    public void setAutoAcknowledge(boolean autoAcknowledge) {
        this.autoAcknowledge = autoAcknowledge;
    }

    /**
     * Sets the meter registry for consumer metrics.
     *
     * @param meterRegistry the meter registry to use
     */
    public void setMeterRegistry(MeterRegistry meterRegistry) {
        consumerMetrics = new ConsumerMetrics(meterRegistry);
    }
}
