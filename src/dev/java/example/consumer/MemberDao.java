package example.consumer;

import org.seasar.doma.Dao;
import org.seasar.doma.Insert;
import org.seasar.doma.Update;

@Dao
public interface MemberDao {
    @Insert
    int insert(Member user);

    @Update
    int update(Member user);
}
