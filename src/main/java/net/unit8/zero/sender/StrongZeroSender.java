package net.unit8.zero.sender;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

public class StrongZeroSender {
    private static final Logger LOG = LoggerFactory.getLogger(StrongZeroSender.class);

    private static final String INSERT_MESSAGE = "INSERT INTO produced_zero(id,type,message) VALUES(?,?,?)";
    private DataSource dataSource;
    private ObjectMapper mapper;
    private Flake flake;
    private ZMQ.Socket notification;
    private ZContext ctx;
    private int thresholdUntilNotify = 3;
    private AtomicInteger updatedCount = new AtomicInteger(0);

    public StrongZeroSender(String notificationAddress, DataSource dataSource, ObjectMapper mapper) {
        ctx = new ZContext();
        notification = ctx.createSocket(ZMQ.PUB);
        notification.setSndHWM(0);
        notification.bind(notificationAddress);

        this.dataSource = dataSource;
        this.mapper = mapper;
        this.flake = new Flake();
    }

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
            // TODO
            throw new RuntimeException(e);
        }
        updatedCount.addAndGet(1);
    }

    public void updated() {
        LOG.info("Update notification!");
        notification.send("update");
    }
}
