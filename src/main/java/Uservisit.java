import com.datastax.driver.mapping.annotations.Table;
import lombok.Data;
import lombok.val;

import static com.google.common.base.Splitter.on;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;

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

    public static Uservisit toUservisit(String record) {
        val values = on(",").split(record).iterator();

        return new Uservisit(
                values.next(),
                values.next(),
                values.next(),
                parseFloat(values.next()),
                values.next(),
                values.next(),
                values.next(),
                values.next(),
                parseInt(values.next())
        );
    }
}
