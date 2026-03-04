package example.consumer;

import org.seasar.doma.Dao;
import org.seasar.doma.Insert;
import org.seasar.doma.Update;

/** DAO for {@link Member} entity operations. */
@Dao
public interface MemberDao {
    /**
     * Inserts a member.
     *
     * @param user the member to insert
     * @return the number of affected rows
     */
    @Insert
    int insert(Member user);

    /**
     * Updates a member.
     *
     * @param user the member to update
     * @return the number of affected rows
     */
    @Update
    int update(Member user);
}
