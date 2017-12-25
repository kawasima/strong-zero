package db.migration.producer;

import org.flywaydb.core.api.migration.jdbc.JdbcMigration;

import java.sql.Connection;
import java.sql.Statement;

public class V2__CreateUsers implements JdbcMigration {
    @Override
    public void migrate(Connection connection) throws Exception {
        try(Statement stmt = connection.createStatement()) {
            String ddl = "CREATE TABLE users(" +
                    "id IDENTITY NOT NULL," +
                    "name VARCHAR(255) NOT NULL," +
                    "email VARCHAR(255) NOT NULL" +
                    ")";
            stmt.execute(ddl);
        }
    }
}
