package example.producer;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.seasar.doma.jdbc.Config;
import org.seasar.doma.jdbc.dialect.Dialect;
import org.seasar.doma.jdbc.dialect.H2Dialect;
import org.seasar.doma.jdbc.tx.LocalTransactionDataSource;
import org.seasar.doma.jdbc.tx.LocalTransactionManager;
import org.seasar.doma.jdbc.tx.TransactionManager;

import javax.sql.DataSource;

public class AppConfig implements Config {
    private final LocalTransactionDataSource dataSource;
    private final TransactionManager transactionManager;
    private final HikariDataSource hikariDataSource;

    public AppConfig() {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl("jdbc:h2:mem:consumer");
        hikariDataSource = new HikariDataSource(hikariConfig);
        dataSource = new LocalTransactionDataSource(hikariDataSource);
        transactionManager = new LocalTransactionManager(
                dataSource.getLocalTransaction(getJdbcLogger()));
    }

    public DataSource getWithoutTxDataSource() {
        return hikariDataSource;
    }

    @Override
    public DataSource getDataSource() {
        return dataSource;
    }

    @Override
    public TransactionManager getTransactionManager() {
        return transactionManager;
    }

    @Override
    public Dialect getDialect() {
        return new H2Dialect();
    }
}
