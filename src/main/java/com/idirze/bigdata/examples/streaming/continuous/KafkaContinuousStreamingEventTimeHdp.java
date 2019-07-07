package com.idirze.bigdata.examples.streaming.continuous;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import scala.concurrent.duration.Duration;

@Slf4j
public class KafkaContinuousStreamingEventTimeHdp {

    //private static final String KAFKA_BROKER_LIST = "sandbox-hdp.hortonworks.com:6667";
    public static final String KAFKA_BROKER_LIST = "192.168.0.24:9092";

    public static final String TOPIC_IN = "topic-in6";
    public static final String TOPIC_OUT = "topic-out";
    private static final String CHECKPOINT_DIR = "/tmp/spark/checkpoint";

    /**
     * Use checkpointing for recovery
     * . option("checkpointLocation", "/some/location/")
     */
    public static void main(String[] args) throws StreamingQueryException {

        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[4]")
                .appName("Stocks-FileStructuredStreaming")
               .config("spark.sql.shuffle.partitions", "1")
                .config("spark.sql.streaming.stateStore.providerClass",
                        "com.idirze.bigdata.examples.streaming.continuous.provider.MemoryStateStoreProvider")
                .getOrCreate();

        Dataset<Row> stockEvents = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BROKER_LIST)
                .option("failOnDataLoss", "true")
                .option("fetchOffset.numRetries", "3")
                .option("startingOffsets", "earliest")
                .option("maxOffsetsPerTrigger", "100")
                .option("kafkaConsumer.pollTimeoutMs", "1000")
                .option("subscribe", TOPIC_IN)
                .option("value.converter.schemas.enable", "false")
                // .option("value.converter", "org.apache.kafka.connect.json.JsonConverter")
                .load();

        // Deduplicate the events to kafka
        StreamingQuery deduplicateQuery = stockEvents
                .selectExpr("CAST(value AS STRING) as value", "key")
               // .withColumn("contentId", hash(col("value")))
               // .dropDuplicates("contentId")// the dedup key
                .dropDuplicates("value")// the dedup key
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BROKER_LIST)
                .option("topic", TOPIC_OUT)
                .option("value.converter.schemas.enable", "false")
                .option("checkpointLocation", CHECKPOINT_DIR)
                .trigger(Trigger.ProcessingTime(Duration.apply("1 s")))
                .start();

        deduplicateQuery.awaitTermination();
    }
}
