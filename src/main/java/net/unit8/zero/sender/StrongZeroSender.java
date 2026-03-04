package net.unit8.zero.sender;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Inserts messages into the outbox table and sends update notifications via ZeroMQ PUB.
 */
public class StrongZeroSender implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(StrongZeroSender.class);

    private static final String INSERT_MESSAGE = "INSERT INTO produced_zero(id,type,message) VALUES(?,?,?)";
    private DataSource dataSource;
    private ObjectMapper mapper;
    private Flake flake;
    private ZMQ.Socket notification;
    private ZContext ctx;
    private int thresholdUntilNotify = 3;
    private AtomicInteger updatedCount = new AtomicInteger(0);

    /**
     * Creates a sender that publishes notifications on the given address.
     *
     * @param notificationAddress ZeroMQ address for update notifications
     * @param dataSource          data source for writing to the outbox table
     * @param mapper              object mapper for serializing message payloads
     */
    public StrongZeroSender(String notificationAddress, DataSource dataSource, ObjectMapper mapper) {
        ctx = new ZContext();
        notification = ctx.createSocket(SocketType.PUB);
        notification.setSndHWM(0);
        notification.bind(notificationAddress);

        this.dataSource = dataSource;
        this.mapper = mapper;
        this.flake = new Flake();
    }

    /**
     * Serializes the object and inserts it into the outbox table.
     * Automatically sends an update notification after every {@code thresholdUntilNotify} messages.
     *
     * @param <T>    the message payload type
     * @param type   the message type identifier
     * @param object the message payload to serialize and store
     */
    public <T> void send(String type, T object) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(INSERT_MESSAGE)) {
            byte[] message = mapper.writeValueAsBytes(object);
            stmt.setString(1, flake.generateId());
            stmt.setString(2, type);
            ByteArrayInputStream bais = new ByteArrayInputStream(message);
            stmt.setBlob(3, bais);
            stmt.execute();
        } catch (SQLException | JsonProcessingException e) {
            throw new IllegalStateException("Failed to send message of type: " + type, e);
        }
        if (updatedCount.incrementAndGet() >= thresholdUntilNotify) {
            updatedCount.set(0);
            updated();
        }
    }

    /** Sends an update notification to pump workers via ZeroMQ PUB. */
    public void updated() {
        LOG.info("Update notification!");
        notification.send("update");
    }

    @Override
    public void close() {
        if (!ctx.isClosed()) {
            ctx.close();
        }
    }
}
