package net.unit8.zero.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import net.unit8.zero.ZeroMessage;
import net.unit8.zero.ZeroMeterRegistry;
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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
    private DataSource dataSource;
    private String producerId;
    private String producerAddress;
    private Clock clock = Clock.systemUTC();

    private ObjectMapper mapper;
    private ConsumerMetrics consumerMetrics = new ConsumerMetrics(new ZeroMeterRegistry());
    private AtomicReference<String> lastId;
    private Map<String, CheckedConsumer> handlers;
    private RetryPolicy retryPolicy;
    private RetryPolicy connectRetryPolicy;

    public StrongZeroConsumer(String producerId, String producerAddress, DataSource dataSource) {
        this.producerId = producerId;
        this.producerAddress = producerAddress;
        this.dataSource = dataSource;

        handlers = new HashMap<>();
        retryPolicy = new RetryPolicy()
                .retryIf(ret -> running.get());
        connectRetryPolicy = new RetryPolicy()
                .withDelay(10, TimeUnit.SECONDS)
                .abortOn(ZMQException.class)
                .retryIf(ret -> Objects.equals(((ZMsg)ret).getFirst().getString(ZMQ.CHARSET), "BUSY"));

        insertSql = String.format(Locale.US, INSERT_SQL, DEFAULT_TABLE_NAME);
        updateSql = String.format(Locale.US, UPDATE_SQL, DEFAULT_TABLE_NAME);
        selectSql = String.format(Locale.US, SELECT_SQL, DEFAULT_TABLE_NAME);

    }

    public void resisterHandler(String type, CheckedConsumer consumer) {
        handlers.put(type, consumer);
    }

    Callable<?> consumer = () -> {
        ZContext ctx = new ZContext();
        ZMQ.Socket socket = ctx.createSocket(ZMQ.REQ);
        socket.connect(producerAddress);
        socket.monitor("inproc://socket.monitor", ZMQ.EVENT_DISCONNECTED);
        ExecutorService executors = Executors.newFixedThreadPool(2);

        Future<?> future = executors.submit(() -> {
            while(!ctx.isClosed()) {
                try {
                    ZMsg msg = Failsafe.with(connectRetryPolicy)
                            .get(() -> {
                                ZMsg request = new ZMsg();
                                request.push(new ZFrame(lastId.get()));
                                request.send(socket);
                                return ZMsg.recvMsg(socket);
                            });
                    String control = msg.popString();
                    if (Objects.equals(control, "SUCCESS")) {
                        List<ZeroMessage> zeroMessages = mapper.readValue(msg.pop().getData(), new TypeReference<List<ZeroMessage>>() {
                        });
                        for (ZeroMessage zeroMessage : zeroMessages) {
                            CheckedConsumer handler = handlers.get(zeroMessage.getType());

                            long start = clock.millis();
                            handler.consume(zeroMessage.getId(), zeroMessage.getMessage());
                            consumerMetrics.getConsumeMessages().increment();
                            consumerMetrics.getConsumeTime().record(clock.millis() - start, TimeUnit.MILLISECONDS);
                        }
                    }
                } catch (ZMQException e) {
                    LOG.warn("Producer {} looks like down.", producerId, e);
                } catch (Exception e) {
                    LOG.warn("Exception occurs when consumes. Retry it.", e);
                }
            }
        });
        ZMQ.Socket monitorSocket = ctx.createSocket(ZMQ.PAIR);
        monitorSocket.connect("inproc://socket.monitor");
        executors.submit(() -> {
            ZMQ.Event event = ZMQ.Event.recv(monitorSocket);
            LOG.info("SOCKET EVENT={} VALUE={}", event.getEvent(), event.getValue());
            ctx.destroy();
        });
        executors.shutdown();
        return future.get();
    };

    public void start() {
        if (mapper == null) mapper = new ObjectMapper(new MessagePackFactory());

        lastId = new AtomicReference<>(getLastId(producerId));
        running.set(true);
        Failsafe.with(retryPolicy)
                .get(consumer);
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
    public void consumed(String id) {
        try(Connection connection = dataSource.getConnection();
            PreparedStatement stmt = connection.prepareStatement(updateSql)) {
            stmt.setString(1, id);
            stmt.setString(2, producerId);
            stmt.executeUpdate();
            connection.commit();
            lastId.set(id);
        } catch (SQLException e) {
            // FIXME
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        running.compareAndSet(true, false);
    }

    public void setObjectMapper(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public void setMeterRegistry(MeterRegistry meterRegistry) {
        consumerMetrics = new ConsumerMetrics(meterRegistry);
    }
}
