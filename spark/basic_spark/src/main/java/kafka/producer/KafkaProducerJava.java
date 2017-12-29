package kafka.producer;

import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class KafkaProducerJava {
    private static Producer<Integer, String> producer;
    private final Properties properties = new Properties();

    public KafkaProducerJava() {
        properties.put("metadata.broker.list", "localhost:9092");
        // properties.put("metadata.broker.list", "192.168.1.3:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("request.required.acks", "1");
        producer = new Producer(new ProducerConfig(properties));
    }

    public static void main(String[] args) {
        new KafkaProducerJava();

        if (args.length < 2) {
            System.out.println("Please enter arguments: topic, message");
            System.exit(0);
        }

        String topic = args[0];
        String msg = args[1];

        try {
            KeyedMessage<Integer, String> data = new KeyedMessage(
                    topic, msg);
            System.out.println(data);
            producer.send(data);
        } catch (Exception e) {
            producer.close();
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

}