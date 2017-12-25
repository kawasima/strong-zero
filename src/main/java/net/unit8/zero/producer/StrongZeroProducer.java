package net.unit8.zero.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import net.unit8.zero.ZeroMeterRegistry;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import org.zeromq.util.ZData;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StrongZeroProducer {
    private static final Logger LOG = LoggerFactory.getLogger(StrongZeroProducer.class);

    private ZContext ctx;
    private ObjectMapper mapper;
    private ZMQ.Socket frontend;
    private ZMQ.Socket backend;
    private ProducerMetrics producerMetrics = new ProducerMetrics(new ZeroMeterRegistry());

    private final String frontendAddress;
    private final String backendAddress;
    private final ExecutorService executor;

    public StrongZeroProducer(String frontendAddress, String backendAddress) {
        this.frontendAddress = frontendAddress;
        this.backendAddress  = backendAddress;

        executor = Executors.newFixedThreadPool(2);

        ctx = new ZContext();
        frontend = ctx.createSocket(ZMQ.ROUTER);
        backend = ctx.createSocket(ZMQ.ROUTER);
    }

    public void start() {
        frontend.bind(frontendAddress);
        frontend.monitor("inproc://frontend.monitor", ZMQ.EVENT_ALL);
        ZMQ.Socket monitorSocket = ctx.createSocket(ZMQ.PAIR);
        monitorSocket.connect("inproc://frontend.monitor");
        executor.submit(() -> {
            while(!Thread.currentThread().isInterrupted()) {
                ZMQ.Event event = ZMQ.Event.recv(monitorSocket);
                LOG.debug("FRONTEND EVENT={} ADDRESS={} VALUE={}", event.getEvent(), event.getAddress(), event.getValue());
            }
        });
        backend.bind(backendAddress);

        if (mapper == null) mapper = new ObjectMapper(new MessagePackFactory());

        ZMQ.Poller items = ctx.createPoller(2);
        items.register(backend, ZMQ.Poller.POLLIN);
        items.register(frontend, ZMQ.Poller.POLLIN);
        Deque<byte[]> workerQueue = new ArrayDeque<>();

        executor.submit(() -> {
            while(!Thread.currentThread().isInterrupted()) {
                items.poll();

                // Backend event
                if (items.pollin(0)) {
                    ZMsg msg = ZMsg.recvMsg(backend, 0);
                    byte[] workerId = msg.pop().getData();
                    workerQueue.push(workerId);
                    byte[] consumerId = msg.pop().getData();
                    if (!Objects.equals(ZData.toString(consumerId), "READY")) {
                        String status = msg.popString();
                        if (Objects.equals(status, "SUCCESS")) {
                            LOG.info("Worker success");
                            ZMsg reply = new ZMsg();
                            reply.add(consumerId);
                            reply.add("");
                            reply.add("SUCCESS");
                            reply.add(msg.pop().getData());
                            reply.send(frontend);
                        } else {
                            LOG.error("Worker error: {}", msg.popString());
                        }
                    } else {
                        LOG.info("Worker connected: {}", workerId);
                    }
                }

                // Frontend event
                if (items.pollin(1)) {
                    ZMsg msg = ZMsg.recvMsg(frontend, 0);
                    byte[] consumerId = msg.pop().getData();
                    msg.pop(); // empty
                    String lastId = msg.popString();

                    if (workerQueue.isEmpty()) {
                        LOG.info("No available workers: {}", consumerId);
                        ZMsg reply = new ZMsg();
                        reply.add(consumerId);
                        reply.add("");
                        reply.add("BUSY");
                        reply.send(frontend);
                    } else {
                        LOG.info("Consumer connected: {}", consumerId);
                        byte[] workerId = workerQueue.pop();
                        ZMsg reply = new ZMsg();
                        reply.add(workerId);
                        reply.add(consumerId);
                        reply.add(lastId);
                        reply.send(backend);
                    }
                }
            }
        });
    }

    public void stop() {
        backend.unbind(backendAddress);
        frontend.unbind(frontendAddress);
        executor.shutdown();
    }

    public void shutdown() {
        backend.close();
        frontend.close();
        ctx.destroy();
    }

    public void setObjectMapper(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public void setProducerMetrics(MeterRegistry registry) {
        producerMetrics = new ProducerMetrics(registry);
    }
}
