package kafka.examples;

import akka.actor.ActorSystem;
import org.apache.kafka.clients.producer.*;
import scala.concurrent.duration.Duration;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class Producer extends Thread {

    private final KafkaProducer<Long, String> producer;
    private final String topic;
    private final Boolean isAsync;


    public Producer(String topic, Boolean isAsync) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.isAsync = isAsync;
    }


    @Override
    public void run() {
//        int messageNo = 1;

        long currentTime;

        Random random = new Random();


        while (true) {
            currentTime = System.currentTimeMillis();

//            try {
//                Thread.sleep(100);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }


            String messageStr = "Message_" + random.nextInt(100);
            long startTime = System.currentTimeMillis();

            if (isAsync) {  // Send asynchronously
                producer.send(new ProducerRecord<>(topic, currentTime, messageStr), new DemoCallBack(startTime, currentTime, messageStr));
            } else {    // Send synchronously
                try {
                    producer.send(new ProducerRecord<>(topic, currentTime, messageStr)).get();
                    System.out.println("Send message: (" + currentTime + ", " + messageStr + ")");
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }

//            ++messageNo;
        }
    }
}

class DemoCallBack implements Callback {

    private final long startTime;
    private final long key;
    private final String message;

    public DemoCallBack(long startTime, long key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        long elapsedTime = System.currentTimeMillis() - startTime;

        if (recordMetadata != null) {
            System.out.println(
                    "message(" + key + ", " + message + ") sent to partition(" + recordMetadata.partition() +
                            "), " +
                            "offset(" + recordMetadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            e.printStackTrace();
        }
    }
}