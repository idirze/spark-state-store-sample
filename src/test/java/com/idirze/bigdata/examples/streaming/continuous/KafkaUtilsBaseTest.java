package com.idirze.bigdata.examples.streaming.continuous;

import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static java.util.Collections.frequency;

@Slf4j
public class KafkaUtilsBaseTest {

    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
            .withBrokers(4);


    public static void produceSmokeDataWithSparseDuplicates(int nbRecords,
                                                            int nbDuplicates,
                                                            String topic,
                                                            String keyPrefix,
                                                            String valuePrefix) {


        new Thread(() -> {
            final KafkaTestUtils kafkaTestUtils = getKafkaTestUtils();

            Random random = new Random();

            List<Integer> duplicates = random.ints(nbDuplicates, 0, nbRecords)
                    .boxed()
                    .collect(Collectors.toList());

            try (final KafkaProducer<String, String> producer
                         = kafkaTestUtils.getKafkaProducer(StringSerializer.class, StringSerializer.class)) {

                for (int i = 0; i < nbRecords; i++) {

                    int nbDups = frequency(duplicates, i);

                    if (nbDups > 0) {
                        int dup = duplicates.get(duplicates.indexOf(i));
                        for (int j = 0; j < nbDups; j++) {
                            producer.send(new ProducerRecord<>(topic, keyPrefix + dup, valuePrefix + dup));
                        }
                    }
                    producer.send(new ProducerRecord<>(topic, keyPrefix + i, valuePrefix + i));
                }

                producer.flush();
            }
        }).start();


    }

    public static void produceSmokeDataWithConsecutiveDuplicates(int nbRecords,
                                                            int nbImmediateDuplicates,
                                                            String topic,
                                                            String keyPrefix,
                                                            String valuePrefix) {


        new Thread(() -> {
            final KafkaTestUtils kafkaTestUtils = getKafkaTestUtils();


            try (final KafkaProducer<String, String> producer
                         = kafkaTestUtils.getKafkaProducer(StringSerializer.class, StringSerializer.class)) {

                for (int i = 0; i < nbRecords; i++) {

                    producer.send(new ProducerRecord<>(topic, keyPrefix + i, valuePrefix + i));
                    for (int j= 0; j< nbImmediateDuplicates; j++){
                        // Send duplicates immediately
                        producer.send(new ProducerRecord<>(topic, keyPrefix + i, valuePrefix + i));
                    }
                }

                producer.flush();
            }
        }).start();


    }

    public static KafkaTestUtils getKafkaTestUtils() {
        return sharedKafkaTestResource.getKafkaTestUtils();
    }

    public static String getKafkaConnectString() {
        return sharedKafkaTestResource.getKafkaConnectString();
    }


}
