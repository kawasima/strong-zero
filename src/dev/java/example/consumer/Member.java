package example.consumer;

import lombok.Data;
import org.seasar.doma.Entity;
import org.seasar.doma.Id;
import org.seasar.doma.Table;

@Data
@Entity
@Table(name = "members")
public class Member {
    @Id
    private Long id;
    private String name;
    private String email;
}
