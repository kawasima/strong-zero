package example.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import net.unit8.zero.producer.StrongZeroProducer;
import net.unit8.zero.sender.StrongZeroSender;
import net.unit8.zero.pump.StrongZeroPump;
import org.flywaydb.core.Flyway;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import javax.sql.DataSource;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Example producer application that publishes User entities via an HTTP endpoint. */
public class ExampleProducer {
    private Undertow undertow;
    private AppConfig appConfig;
    private StrongZeroSender sender;

    private HttpHandler handler = new HttpHandler() {
        @Override
        public void handleRequest(HttpServerExchange exchange) throws Exception {
            if (exchange.isInIoThread()) {
                exchange.dispatch(this);
                return;
            }

            exchange.startBlocking();
            User user = new User();
            user.setName("kawasima");
            user.setEmail("kawasima@example.org");
            UserDao userDao = new UserDaoImpl(appConfig);

            appConfig.getTransactionManager().required(() -> {
                userDao.insert(user);
                sender.send("USER", user);
            });

            exchange.getResponseSender().send("OK");
        }
    };

    /** Initializes the producer, pump workers, and HTTP server. */
    public ExampleProducer() {
        ExecutorService workerThreadPool = Executors.newFixedThreadPool(2);

        appConfig = new AppConfig();
        appConfig.getTransactionManager()
                .required(() -> migrate(appConfig.getDataSource()));

        String frontendAddress = "tcp://127.0.0.1:5959";
        String backendAddress  = "ipc://backend";
        String notificationAddress = "ipc://notification";
        ObjectMapper mapper = new ObjectMapper(new MessagePackFactory());
        StrongZeroProducer producer = new StrongZeroProducer(frontendAddress, backendAddress);
        producer.start();

        workerThreadPool.submit(new StrongZeroPump(backendAddress, notificationAddress, appConfig.getWithoutTxDataSource()));
        workerThreadPool.submit(new StrongZeroPump(backendAddress, notificationAddress, appConfig.getWithoutTxDataSource()));

        undertow = Undertow.builder()
                .addHttpListener(3000, "0.0.0.0")
                .setHandler(handler)
                .build();
        undertow.start();
        sender = new StrongZeroSender(notificationAddress, appConfig.getDataSource(), mapper);
    }

    /**
     * Runs Flyway migrations for the producer database.
     *
     * @param dataSource the data source to migrate
     */
    public void migrate(DataSource dataSource) {
        Flyway.configure()
                .locations("classpath:db/migration/producer")
                .dataSource(dataSource)
                .load()
                .migrate();
    }

    /**
     * Entry point for the example producer.
     *
     * @param args command-line arguments (unused)
     */
    public static void main(String[] args) {
        new ExampleProducer();
    }
}
