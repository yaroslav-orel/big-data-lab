import com.datastax.driver.core.Cluster;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.function.Function;

import static com.google.common.base.Splitter.on;
import static com.google.common.collect.Streams.stream;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;
import static java.util.Collections.singletonList;

public class Application {

    public static void main(String[] args) {
        try(
            val kafkaConsumer = initConsumer();
            val cassandraCluster = initCluster()
        ){
            val mapper = initMapper(cassandraCluster);

            while (true){
                val records = kafkaConsumer.poll(1000);
                stream(records)
                        .map(toUservisit())
                        .forEach(mapper::save);
            }
        }catch (Throwable e){
            e.printStackTrace();
        }
    }

    private static KafkaConsumer<String, String> initConsumer() {
        val props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "main");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        val conusmer = new KafkaConsumer<String, String>(props);

        conusmer.subscribe(singletonList("uservisits"));
        return conusmer;
    }

    private static Cluster initCluster() {
        return Cluster.builder().addContactPoint("127.0.0.1").build();
    }

    private static Mapper<Uservisit> initMapper(Cluster cassandraCluster) {
        val session = cassandraCluster.connect();
        val manager = new MappingManager(session);
        return manager.mapper(Uservisit.class);
    }

    private static Function<ConsumerRecord<String, String>, Uservisit> toUservisit() {
        return record -> {
            val values = on(",").split(record.value()).iterator();

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
        };
    }
}
