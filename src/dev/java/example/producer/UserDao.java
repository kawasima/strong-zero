package example.producer;

import org.seasar.doma.Dao;
import org.seasar.doma.Insert;
import org.seasar.doma.Update;

@Dao
public interface UserDao {
    @Insert
    int insert(User user);

    @Update
    int update(User user);
}
