package de.mastery.jvm06;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;

/** Reference: produce values, consume them back, print a tally. No sleep(). */
public class KafkaRoundTrip {

    public static void main(String[] args) {
        String bootstrap = args[0];
        String topic = args[1];
        List<String> values = Arrays.asList(args).subList(2, args.length);

        Properties pp = new Properties();
        pp.put("bootstrap.servers", bootstrap);
        pp.put("key.serializer", StringSerializer.class.getName());
        pp.put("value.serializer", StringSerializer.class.getName());
        pp.put("acks", "all");
        try (Producer<String, String> producer = new KafkaProducer<>(pp)) {
            for (String v : values) {
                producer.send(new ProducerRecord<>(topic, v, v));
            }
            producer.flush();
        }

        Properties cp = new Properties();
        cp.put("bootstrap.servers", bootstrap);
        cp.put("key.deserializer", StringDeserializer.class.getName());
        cp.put("value.deserializer", StringDeserializer.class.getName());
        cp.put("group.id", "jvm06-" + UUID.randomUUID());
        cp.put("auto.offset.reset", "earliest");
        cp.put("enable.auto.commit", "false");

        Map<String, Long> tally = new TreeMap<>();
        try (Consumer<String, String> consumer = new KafkaConsumer<>(cp)) {
            consumer.subscribe(Collections.singletonList(topic));
            long deadline = System.currentTimeMillis() + 20_000;
            int got = 0;
            while (got < values.size() && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, String> r : records) {
                    tally.merge(r.value(), 1L, Long::sum);
                    got++;
                }
            }
        }

        StringBuilder sb = new StringBuilder("TALLY\n");
        for (Map.Entry<String, Long> e : tally.entrySet()) {
            sb.append(e.getKey()).append('=').append(e.getValue()).append('\n');
        }
        System.out.print(sb);
    }
}
