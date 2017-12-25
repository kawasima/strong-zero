package net.unit8.zero.pump;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import net.unit8.zero.ZeroMessage;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author kawasima
 */
public class StrongZeroPump implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(StrongZeroPump.class);

    private static final String SELECT_MESSAGES = "SELECT id, type, message FROM produced_zero "
            + " WHERE id > ?"
            + " ORDER BY id ";

    private final String backendAddress;
    private final String notificationAddress;

    private final DataSource dataSource;
    private final ZContext ctx;
    private final ZMQ.Socket backend;
    private final ZMQ.Socket notification;

    private ObjectMapper mapper;
    private int batchSize = 100;

    @SuppressWarnings("unchecked")
    private RetryPolicy retryPolicy = new RetryPolicy()
            .abortOn(SQLException.class)
            .retryIf(msgs -> ((List<ZeroMessage>) msgs).isEmpty())
            .withMaxRetries(3); // Avoid a infinite loop when consumer is disconnected

    public StrongZeroPump(String backendAddress, String notificationAddress, DataSource dataSource) {
        this.backendAddress = backendAddress;
        this.notificationAddress = notificationAddress;
        this.dataSource = dataSource;

        ctx = new ZContext();
        backend = ctx.createSocket(ZMQ.DEALER);
        notification = ctx.createSocket(ZMQ.SUB);
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
            AtomicLong pollWaitTime = new AtomicLong(10_000);
            reply.add(consumerId);
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement stmt = connection.prepareStatement(
                         SELECT_MESSAGES + " LIMIT " + (batchSize + 1))) {
                List<ZeroMessage> messages = Failsafe.with(retryPolicy)
                        .get(() -> {
                            List<ZeroMessage> res = new ArrayList<>();
                            stmt.setString(1, lastId);
                            int cnt = 0;
                            try (ResultSet rs = stmt.executeQuery()) {
                                while (cnt < batchSize && rs.next()) {
                                    ZeroMessage message = new ZeroMessage(
                                            rs.getString(1),
                                            rs.getString(2),
                                            rs.getBytes(3));
                                    res.add(message);
                                    cnt++;
                                }
                                if (cnt == 0) {
                                    if (poller.poll(pollWaitTime.get()) != 0) {
                                        ZMsg.recvMsg(notification, false);
                                    }
                                } else if (cnt > 0 && !rs.isAfterLast()) {
                                    pollWaitTime.set(0);
                                }
                                return res;
                            }
                        });

                byte[] replyBody = mapper.writeValueAsBytes(messages);
                reply.add("SUCCESS");
                reply.add(replyBody);
                LOG.info("fetch: {}", messages);
                reply.send(backend);
                if (poller.poll(pollWaitTime.get()) != 0) {
                    ZMsg.recvMsg(notification, false);
                    LOG.info("Receive an update notification");
                }
            } catch (Exception e) {
                reply.add("ERROR");
                reply.add(e.getMessage());
                reply.send(backend);
            }
        }
    }

    public void setObjectMapper(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
