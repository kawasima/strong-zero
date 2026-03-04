package db.migration.consumer;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

import java.sql.Statement;

/** Creates the members table for the consumer example. */
public class V2__CreateMembers extends BaseJavaMigration {
    /** Creates a new migration instance. */
    public V2__CreateMembers() {}

    @Override
    public void migrate(Context context) throws Exception {
        try(Statement stmt = context.getConnection().createStatement()) {
            String ddl = "CREATE TABLE members(" +
                    "id BIGINT NOT NULL," +
                    "name VARCHAR(255) NOT NULL," +
                    "email VARCHAR(255) NOT NULL" +
                    ")";
            stmt.execute(ddl);
        }
    }
}
