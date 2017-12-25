package db.migration.producer;

import org.flywaydb.core.api.migration.jdbc.JdbcMigration;

import java.sql.Connection;
import java.sql.Statement;

public class V1__CreateProducedZero implements JdbcMigration {
    @Override
    public void migrate(Connection connection) throws Exception {
        try(Statement stmt = connection.createStatement()) {
            String ddl = "CREATE TABLE produced_zero (" +
                    "id VARCHAR(32) NOT NULL," +
                    "type VARCHAR(32) NOT NULL," +
                    "message BLOB NOT NULL" +
                    ")";
            stmt.execute(ddl);
        }
    }
}
