package net.unit8.zero.producer;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;
import org.zeromq.util.ZData;

import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Routes messages between consumers and pump workers using the ZeroMQ ROUTER pattern.
 * Acts as a broker in the transactional outbox architecture.
 */
public class StrongZeroProducer {
    private static final Logger LOG = LoggerFactory.getLogger(StrongZeroProducer.class);

    private final ZContext ctx;
    private final ZMQ.Socket frontend;
    private final ZMQ.Socket backend;
    private final Deque<byte[]> workerQueue = new ConcurrentLinkedDeque<>();
    @SuppressWarnings("unused") // Gauge registration happens in constructor as side effect
    private ProducerMetrics producerMetrics;

    private final String frontendAddress;
    private final String backendAddress;
    private final ExecutorService executor;

    /**
     * Creates a producer that binds to the given frontend and backend addresses.
     *
     * @param frontendAddress ZeroMQ address for consumer connections
     * @param backendAddress  ZeroMQ address for pump worker connections
     */
    public StrongZeroProducer(String frontendAddress, String backendAddress) {
        this.frontendAddress = frontendAddress;
        this.backendAddress  = backendAddress;

        executor = Executors.newFixedThreadPool(2);

        ctx = new ZContext();
        frontend = ctx.createSocket(SocketType.ROUTER);
        backend = ctx.createSocket(SocketType.ROUTER);
        producerMetrics = new ProducerMetrics(new SimpleMeterRegistry(), workerQueue);
    }

    /** Binds sockets and starts the message routing loop. */
    public void start() {
        frontend.bind(frontendAddress);
        frontend.monitor("inproc://frontend.monitor", ZMQ.EVENT_ALL);
        ZMQ.Socket monitorSocket = ctx.createSocket(SocketType.PAIR);
        monitorSocket.connect("inproc://frontend.monitor");
        executor.submit(() -> {
            while(!Thread.currentThread().isInterrupted()) {
                ZEvent event = ZEvent.recv(monitorSocket);
                LOG.debug("FRONTEND EVENT={} ADDRESS={} VALUE={}", event.getEvent(), event.getAddress(), event.getValue());
            }
        });
        backend.bind(backendAddress);

        ZMQ.Poller items = ctx.createPoller(2);
        items.register(backend, ZMQ.Poller.POLLIN);
        items.register(frontend, ZMQ.Poller.POLLIN);

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
                        ZMsg reply = new ZMsg();
                        reply.add(consumerId);
                        reply.add("");
                        if (Objects.equals(status, "SUCCESS")) {
                            LOG.info("Worker success");
                            reply.add("SUCCESS");
                            reply.add(msg.pop().getData());
                        } else {
                            String errorMsg = msg.popString();
                            LOG.error("Worker error: {}", errorMsg);
                            reply.add("ERROR");
                            reply.add(errorMsg != null ? errorMsg : "Unknown worker error");
                        }
                        reply.send(frontend);
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

    /** Stops the routing loop, closes sockets and the ZeroMQ context. */
    public void stop() {
        executor.shutdownNow();
        backend.close();
        frontend.close();
        ctx.close();
    }

    /**
     * Sets the meter registry for producer metrics.
     *
     * @param registry the meter registry to use
     */
    public void setProducerMetrics(MeterRegistry registry) {
        producerMetrics = new ProducerMetrics(registry, workerQueue);
    }
}
