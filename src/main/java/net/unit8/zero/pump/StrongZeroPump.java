package net.unit8.zero.pump;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import net.unit8.zero.ZeroMessage;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Fetches messages from the outbox table and sends them to the producer via ZeroMQ.
 * Runs as a worker thread in the transactional outbox pipeline.
 *
 * @author kawasima
 */
public class StrongZeroPump implements Runnable, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(StrongZeroPump.class);

    private static final String SELECT_MESSAGES = "SELECT id, type, message FROM produced_zero "
            + " WHERE id > ?"
            + " ORDER BY id"
            + " LIMIT ?";

    private final String backendAddress;
    private final String notificationAddress;

    private final DataSource dataSource;
    private final ZContext ctx;
    private final ZMQ.Socket backend;
    private final ZMQ.Socket notification;

    private ObjectMapper mapper;
    private int batchSize = 100;

    private final RetryPolicy<List<ZeroMessage>> retryPolicy = RetryPolicy.<List<ZeroMessage>>builder()
            .handle(SQLException.class)
            .withDelay(Duration.ofSeconds(1))
            .withMaxRetries(3)
            .build();

    /**
     * Creates a pump worker.
     *
     * @param backendAddress      ZeroMQ address of the producer backend
     * @param notificationAddress ZeroMQ address for update notifications
     * @param dataSource          data source for reading outbox messages
     */
    public StrongZeroPump(String backendAddress, String notificationAddress, DataSource dataSource) {
        this.backendAddress = backendAddress;
        this.notificationAddress = notificationAddress;
        this.dataSource = dataSource;

        ctx = new ZContext();
        backend = ctx.createSocket(SocketType.DEALER);
        notification = ctx.createSocket(SocketType.SUB);
        notification.subscribe(ZMQ.SUBSCRIPTION_ALL);
    }

    @Override
    public void run() {
        if (mapper == null) mapper = new ObjectMapper(new MessagePackFactory());

        backend.connect(backendAddress);
        notification.connect(notificationAddress);
        backend.send("READY");

        ZMQ.Poller poller = ctx.createPoller(1);
        poller.register(notification, ZMQ.Poller.POLLIN);

        LOG.info("Worker start");
        while(!Thread.currentThread().isInterrupted()) {
            ZMsg request = ZMsg.recvMsg(backend);
            byte[] consumerId = request.pop().getData();
            String lastId = request.popString();

            ZMsg reply = new ZMsg();
            reply.add(consumerId);
            try {
                List<ZeroMessage> messages = fetchMessages(lastId);

                byte[] replyBody = mapper.writeValueAsBytes(messages);
                reply.add("SUCCESS");
                reply.add(replyBody);
                LOG.info("fetch: {}", messages);
                reply.send(backend);

                boolean hasMore = messages.size() >= batchSize;
                if (!hasMore) {
                    waitForNotification(poller);
                }
            } catch (Exception e) {
                LOG.error("Failed to fetch messages for lastId={}", lastId, e);
                reply.add("ERROR");
                reply.add(e.getMessage() != null ? e.getMessage() : "Unknown error");
                reply.send(backend);
            }
        }
    }

    private List<ZeroMessage> fetchMessages(String lastId) throws Exception {
        return Failsafe.with(retryPolicy)
                .get(() -> {
                    try (Connection connection = dataSource.getConnection();
                         PreparedStatement stmt = connection.prepareStatement(SELECT_MESSAGES)) {
                        List<ZeroMessage> res = new ArrayList<>();
                        stmt.setString(1, lastId);
                        stmt.setInt(2, batchSize);
                        try (ResultSet rs = stmt.executeQuery()) {
                            while (rs.next()) {
                                res.add(new ZeroMessage(
                                        rs.getString(1),
                                        rs.getString(2),
                                        rs.getBytes(3)));
                            }
                        }
                        return res;
                    }
                });
    }

    private static final long NOTIFICATION_POLL_TIMEOUT_MS = 10_000;

    private void waitForNotification(ZMQ.Poller poller) {
        if (poller.poll(NOTIFICATION_POLL_TIMEOUT_MS) != 0) {
            ZMsg.recvMsg(notification, false);
            LOG.info("Receive an update notification");
        } else {
            LOG.debug("Notification poll timed out, will re-query database");
        }
    }

    @Override
    public void close() {
        if (!ctx.isClosed()) {
            ctx.close();
        }
    }

    /**
     * Sets the ObjectMapper used for serializing messages.
     *
     * @param mapper the object mapper to use
     */
    public void setObjectMapper(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * Sets the maximum number of messages to fetch per batch.
     *
     * @param batchSize the batch size
     */
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
