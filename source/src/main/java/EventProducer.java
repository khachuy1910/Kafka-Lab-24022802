import org.apache.kafka.clients.producer.*;
import java.util.*;

public class EventProducer {
    public static void main(String[] args) {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092,localhost:9094");
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(p);
             Scanner sc = new Scanner(System.in)) {
            System.out.println("Producer started. Type 'exit' to quit.");
            while (true) {
                System.out.print("> ");
                String msg = sc.nextLine();
                if ("exit".equalsIgnoreCase(msg)) break;
                producer.send(new ProducerRecord<>("network-events", msg), (m, e) -> {
                    if (e == null)
                        System.out.printf("Sent: partition=%d, offset=%d%n", m.partition(), m.offset());
                    else
                        e.printStackTrace();
                });
            }
            producer.flush();
        }
    }
}
