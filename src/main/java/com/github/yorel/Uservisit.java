package com.github.yorel;

import com.datastax.driver.mapping.annotations.Table;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.val;

import java.io.Serializable;

import static com.google.common.base.Splitter.on;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;

@Value
@AllArgsConstructor
@Table(keyspace = "lab", name = "uservisit")
public class Uservisit implements Serializable {
    String sourceip;
    String desturl;
    String visitdate;
    Float adrevenue;
    String useragent;
    String countrycode;
    String languagecode;
    String searchword;
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
