import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PubSubPollerTest {
    private static final Logger LOG = LoggerFactory.getLogger(PubSubPollerTest.class);
    @Test
    public void test() throws InterruptedException {
        ZContext ctx = new ZContext();
        ZMQ.Socket pubSock = ctx.createSocket(ZMQ.PUB);
        pubSock.bind("ipc://pubsub.ipc");
        ZMQ.Socket subSock = ctx.createSocket(ZMQ.SUB);
        //subSock.subscribe(ZMQ.SUBSCRIPTION_ALL);
        subSock.connect("ipc://pubsub.ipc");

        ExecutorService executorService = Executors.newCachedThreadPool();

        ZMQ.Poller poller = ctx.createPoller(1);
        poller.register(subSock);

        executorService.submit(() -> {
            int ret = poller.poll(30_000);
            if (poller.pollin(0)) {
                ZMsg msg = ZMsg.recvMsg(subSock, false);
                System.out.println(msg);
            }
        });

        zmq.ZMQ.sleep(1);
        executorService.submit(() -> {
            pubSock.send("Hello!!");
            LOG.info("Publish message");
        });

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);
    }
}
