package example.producer;

import org.seasar.doma.Dao;
import org.seasar.doma.Insert;
import org.seasar.doma.Update;

/** DAO for {@link User} entity operations. */
@Dao
public interface UserDao {
    /**
     * Inserts a user.
     *
     * @param user the user to insert
     * @return the number of affected rows
     */
    @Insert
    int insert(User user);

    /**
     * Updates a user.
     *
     * @param user the user to update
     * @return the number of affected rows
     */
    @Update
    int update(User user);
}
