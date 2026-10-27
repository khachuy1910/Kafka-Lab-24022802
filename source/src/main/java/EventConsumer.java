import org.apache.kafka.clients.consumer.*;
import java.time.Duration;
import java.util.*;

public class EventConsumer {
    public static void main(String[] args) {
        String group = args.length > 0 ? args[0] : "network-lab-group";
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092,localhost:9094");
        p.put("group.id", group);
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("auto.offset.reset", "earliest");
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p)) {
            c.subscribe(Collections.singletonList("network-events"));
            System.out.println("Consumer started... group=" + group);
            while (true) {
                for (ConsumerRecord<String, String> r : c.poll(Duration.ofMillis(1000)))
                    System.out.printf("Received=%s | partition=%d | offset=%d%n",
                            r.value(), r.partition(), r.offset());
            }
        }
    }
}
