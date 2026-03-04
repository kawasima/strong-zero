package example.producer;

import lombok.Data;
import org.seasar.doma.*;

/** Producer-side entity representing a user. */
@Data
@Entity
@Table(name = "users")
public class User {
    /** Creates a new User instance. */
    public User() {}

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String name;
    private String email;
}
