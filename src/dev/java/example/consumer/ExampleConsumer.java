package example.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.unit8.zero.consumer.StrongZeroConsumer;
import org.flywaydb.core.Flyway;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import javax.sql.DataSource;

/** Example consumer application that replicates User entities as Member entities. */
public class ExampleConsumer {
    /** Creates a new ExampleConsumer instance. */
    public ExampleConsumer() {}

    /** Runs Flyway migrations for the consumer database.
     *
     * @param dataSource the data source to migrate
     */
    public static void migrate(DataSource dataSource) {
        Flyway.configure()
                .locations("classpath:db/migration/consumer")
                .dataSource(dataSource)
                .load()
                .migrate();
    }

    /**
     * Entry point for the example consumer.
     *
     * @param args command-line arguments (unused)
     */
    public static void main(String[] args) {
        final AppConfig appConfig = new AppConfig();
        appConfig.getTransactionManager().required(() -> migrate(appConfig.getDataSource()));

        String producerAddress ="tcp://127.0.0.1:5959";
        ObjectMapper mapper = new ObjectMapper(new MessagePackFactory());

        StrongZeroConsumer strongZeroConsumer = new StrongZeroConsumer("producer1", producerAddress, appConfig.getDataSource());
        strongZeroConsumer.setObjectMapper(mapper);

        MemberDao memberDao = new MemberDaoImpl(appConfig);
        strongZeroConsumer.registerHandler("USER", (id, msg) -> {
            Member user = mapper.readValue(msg, Member.class);
            appConfig.getTransactionManager().required(() -> {
                if (memberDao.update(user) == 0) {
                    memberDao.insert(user);
                }
            });
        });

        appConfig.getTransactionManager().required(strongZeroConsumer::start);
    }
}
