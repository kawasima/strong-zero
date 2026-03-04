package db.migration.producer;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

import java.sql.Statement;

/** Creates the users table for the producer example. */
public class V2__CreateUsers extends BaseJavaMigration {
    /** Creates a new migration instance. */
    public V2__CreateUsers() {}

    @Override
    public void migrate(Context context) throws Exception {
        try(Statement stmt = context.getConnection().createStatement()) {
            String ddl = "CREATE TABLE users(" +
                    "id IDENTITY NOT NULL," +
                    "name VARCHAR(255) NOT NULL," +
                    "email VARCHAR(255) NOT NULL" +
                    ")";
            stmt.execute(ddl);
        }
    }
}
