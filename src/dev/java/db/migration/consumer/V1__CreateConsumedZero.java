package db.migration.consumer;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

import java.sql.Statement;

/** Creates the consumed_zero table for tracking consumed message positions. */
public class V1__CreateConsumedZero extends BaseJavaMigration {
    /** Creates a new migration instance. */
    public V1__CreateConsumedZero() {}

    @Override
    public void migrate(Context context) throws Exception {
        try(Statement stmt = context.getConnection().createStatement()) {
            String ddl = "CREATE TABLE consumed_zero("
                    + "producer_id VARCHAR(16) NOT NULL PRIMARY KEY,"
                    + "last_id VARCHAR(32) NOT NULL"
                    + ")";
            stmt.execute(ddl);
        }
    }
}
