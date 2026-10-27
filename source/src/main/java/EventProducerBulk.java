import org.apache.kafka.clients.producer.*;
import java.util.*;

public class EventProducerBulk {
    public static void main(String[] args) {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092,localhost:9094");
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
            long start = System.currentTimeMillis();
            for (int i = 1; i <= 10000; i++) {
                producer.send(new ProducerRecord<>("network-events", "event-" + i));
            }
            producer.flush();
            long end = System.currentTimeMillis();
            double seconds = (end - start) / 1000.0;
            System.out.println("Elapsed = " + seconds + " s");
            System.out.println("Approx. throughput = " + (10000 / seconds) + " events/s");
        }
    }
}
