import com.datastax.driver.mapping.annotations.Table;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.val;

import static com.google.common.base.Splitter.on;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;

@Value
@AllArgsConstructor
@Table(keyspace = "lab", name = "uservisit")
public class Uservisit {
    String sourceIP;
    String destURL;
    String visitDate;
    Float adRevenue;
    String userAgent;
    String countryCode;
    String languageCode;
    String searchWord;
    Integer duration;

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
