import com.datastax.driver.mapping.annotations.Table;
import lombok.Data;
import lombok.NonNull;

import java.time.LocalDate;

@Data
@Table(keyspace = "lab", name = "uservisit")
public class Uservisit {
    private final String sourceIP;
    private final String destURL;
    private final String visitDate;
    private final Float adRevenue;
    private final String userAgent;
    private final String countryCode;
    private final String languageCode;
    private final String searchWord;
    private final Integer duration;
}
