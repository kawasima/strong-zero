package net.unit8.zero;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

public class PubSubPollerTest {
    private static final Logger LOG = LoggerFactory.getLogger(PubSubPollerTest.class);

    @Test
    public void testPubSubWithPoller() throws Exception {
        try (ZContext ctx = new ZContext()) {
            ZMQ.Socket pubSock = ctx.createSocket(SocketType.PUB);
            pubSock.bind("inproc://pubsub-test");
            ZMQ.Socket subSock = ctx.createSocket(SocketType.SUB);
            subSock.subscribe(ZMQ.SUBSCRIPTION_ALL);
            subSock.connect("inproc://pubsub-test");

            ExecutorService executorService = Executors.newCachedThreadPool();

            ZMQ.Poller poller = ctx.createPoller(1);
            poller.register(subSock);

            Future<ZMsg> receiveFuture = executorService.submit(() -> {
                poller.poll(30_000);
                if (poller.pollin(0)) {
                    return ZMsg.recvMsg(subSock, false);
                }
                return null;
            });

            // Allow time for subscription to propagate
            Thread.sleep(200);

            executorService.submit(() -> {
                pubSock.send("Hello!!");
                LOG.info("Publish message");
            });

            ZMsg received = receiveFuture.get(10, TimeUnit.SECONDS);
            assertNotNull(received, "Should have received a message");
            assertEquals("Hello!!", received.getFirst().getString(ZMQ.CHARSET));

            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
