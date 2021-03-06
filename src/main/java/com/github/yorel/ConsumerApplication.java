package com.github.yorel;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import lombok.Cleanup;
import lombok.experimental.UtilityClass;
import lombok.val;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

import static com.google.common.collect.Streams.stream;
import static java.util.Collections.singletonList;

@UtilityClass
public class ConsumerApplication {

    public void main(String[] args) {
        @Cleanup val kafkaConsumer = initConsumer();
        @Cleanup val cassandraCluster = initCluster();
        @Cleanup val session = initSession(cassandraCluster);

        val mapper = initMapper(session);

        while (true){
            val records = kafkaConsumer.poll(1000);

            stream(records)
                    .map(ConsumerRecord::value)
                    .map(Uservisit::toUservisit)
                    .forEach(mapper::save);
        }
    }

    private Consumer<String, String> initConsumer() {
        val props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "main");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        val conusmer = new KafkaConsumer<String, String>(props);

        conusmer.subscribe(singletonList("uservisits"));
        return conusmer;
    }

    private Cluster initCluster() {
        return Cluster.builder().addContactPoint("127.0.0.1").build();
    }

    private Session initSession(Cluster cluster) {
        return cluster.connect();
    }

    private Mapper<Uservisit> initMapper(Session session) {
        val manager = new MappingManager(session);
        return manager.mapper(Uservisit.class);
    }

}
