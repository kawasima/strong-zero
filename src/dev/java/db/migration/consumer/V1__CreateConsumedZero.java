package db.migration.consumer;

import org.flywaydb.core.api.migration.jdbc.JdbcMigration;

import java.sql.Connection;
import java.sql.Statement;

public class V1__CreateConsumedZero implements JdbcMigration {
    @Override
    public void migrate(Connection connection) throws Exception {
        try(Statement stmt = connection.createStatement()) {
            String ddl = "CREATE TABLE consumed_zero("
                    + "producer_id VARCHAR(16) NOT NULL,"
                    + "last_id VARCHAR(32) NOT NULL,"
                    + ")";
            stmt.execute(ddl);
        }
    }
}
