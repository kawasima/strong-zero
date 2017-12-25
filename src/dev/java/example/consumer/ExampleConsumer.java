package example.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.unit8.zero.consumer.StrongZeroConsumer;
import org.flywaydb.core.Flyway;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import javax.sql.DataSource;

public class ExampleConsumer {
    public static void migrate(DataSource dataSource) {
        Flyway flyway = new Flyway();
        flyway.setLocations("classpath:db/migration/consumer");
        flyway.setDataSource(dataSource);
        flyway.migrate();
    }

    public static void main(String[] args) {
        final AppConfig appConfig = new AppConfig();
        appConfig.getTransactionManager().required(() -> migrate(appConfig.getDataSource()));

        String producerAddress ="tcp://127.0.0.1:5959";
        ObjectMapper mapper = new ObjectMapper(new MessagePackFactory());

        StrongZeroConsumer strongZeroConsumer = new StrongZeroConsumer("producer1", producerAddress, appConfig.getDataSource());
        strongZeroConsumer.setObjectMapper(mapper);

        MemberDao memberDao = new MemberDaoImpl(appConfig);
        strongZeroConsumer.resisterHandler("USER", (id, msg) -> {
            Member user = mapper.readValue(msg, Member.class);
            appConfig.getTransactionManager().required(() -> {
                if (memberDao.update(user) == 0) {
                    memberDao.insert(user);
                }
                strongZeroConsumer.consumed(id);
            });
        });

        appConfig.getTransactionManager().required(strongZeroConsumer::start);
    }
}
