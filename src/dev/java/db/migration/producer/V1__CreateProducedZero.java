package db.migration.producer;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

import java.sql.Statement;

/** Creates the produced_zero outbox table. */
public class V1__CreateProducedZero extends BaseJavaMigration {
    /** Creates a new migration instance. */
    public V1__CreateProducedZero() {}

    @Override
    public void migrate(Context context) throws Exception {
        try(Statement stmt = context.getConnection().createStatement()) {
            String ddl = "CREATE TABLE produced_zero (" +
                    "id VARCHAR(32) NOT NULL PRIMARY KEY," +
                    "type VARCHAR(32) NOT NULL," +
                    "message BLOB NOT NULL" +
                    ")";
            stmt.execute(ddl);
        }
    }
}
