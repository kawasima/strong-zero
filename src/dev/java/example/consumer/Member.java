package example.consumer;

import lombok.Data;
import org.seasar.doma.Entity;
import org.seasar.doma.Id;
import org.seasar.doma.Table;

/** Consumer-side entity representing a replicated member. */
@Data
@Entity
@Table(name = "members")
public class Member {
    /** Creates a new Member instance. */
    public Member() {}

    @Id
    private Long id;
    private String name;
    private String email;
}
