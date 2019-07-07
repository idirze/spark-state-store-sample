package com.idirze.bigdata.examples.streaming.continuous.provider;

import com.idirze.bigdata.examples.streaming.continuous.KafkaUtilsBaseTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.hash;
import static org.apache.spark.sql.streaming.OutputMode.Append;
import static org.assertj.core.api.Assertions.assertThat;

public class StreamingDedupeTest extends KafkaUtilsBaseTest {

    private static final String TOPIC_IN_SPARSE = "topic-in-sparse-v1";
    private static final String TOPIC_IN_IMMEDIATE = "topic-in-immediate-v1";
    private static SparkSession spark;

    @Rule
    public TemporaryFolder CHECKPOINT_DIR = new TemporaryFolder();

    @BeforeAll
    public static void setUpClass() {
        spark = SparkSession
                .builder()
                .appName("ContinuousStreamingDedupTest")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "3")
                // Configure the state store provider
                .config("spark.sql.streaming.stateStore.providerClass",
                        "com.idirze.bigdata.examples.streaming.continuous.provider.CustomStateStoreProvider")
               // Configure the state store backend class (in memory, Hbase, etc)
                .config("spark.sql.streaming.stateStore.stateStoreClass", "com.idirze.bigdata.examples.streaming.continuous.state.memory.MemoryStateStore")

                .getOrCreate();

        // Create the input topic
        getKafkaTestUtils().createTopic(TOPIC_IN_SPARSE, 3, (short) 1);
        // Create the input topic
        getKafkaTestUtils().createTopic(TOPIC_IN_IMMEDIATE, 3, (short) 1);

    }


    @Test
    @DisplayName("Ensure the sparse duplicates are removed from the output")
    public void sparse_duplicates_should_be_removed_test() {

        int nbRecords = 1000;
        int nbDuplicates = 245;

        // Insert some smoke date into the input kafka topic:
        // #nbRecords + nbDuplicates records into the input topic
        produceSmokeDataWithSparseDuplicates(nbRecords, nbDuplicates, TOPIC_IN_SPARSE, "key", "value");

        // Create an input stream from the input kafka topic
        Dataset<Row> inStream = createStreamingDataFrame(TOPIC_IN_SPARSE);
        List<Row> inResult = collectAsList(inStream, "InputStreamSparse");

        assertThat(inResult.size()).
                isEqualTo(nbRecords + nbDuplicates);

        // Deduplicate the stream
        Dataset<Row> outStream = deduplicateStream(inStream);

        // Collect the result
        List<Row> result = collectAsList(outStream, "OutputStreamSparse");

        List<Row> expected = new ArrayList<>();

        IntStream
                .range(0, nbRecords)
                .forEach(i -> expected.add(RowFactory.create("key" + i, "value" + i)));

        assertThat(result.size()).
                isEqualTo(expected.size());

        assertThat(result)
                .containsExactlyInAnyOrder(expected.toArray(new Row[result.size()]));

    }

    @Test
    @DisplayName("Ensure the duplicates that are inserted immediately are removed from the output")
    public void immediate_duplicates_should_be_removed_test() {

        int nbRecords = 1000;
        int nbImmDuplicates = 10;

        // Insert some smoke date into the input kafka topic:
        // #nbRecords + nbDuplicates records into the input topic
        produceSmokeDataWithConsecutiveDuplicates(nbRecords, nbImmDuplicates, TOPIC_IN_IMMEDIATE, "key", "value");

        // Create an input stream from the input kafka topic
        Dataset<Row> inStream = createStreamingDataFrame(TOPIC_IN_IMMEDIATE);
        List<Row> inResult = collectAsList(inStream, "InputStreamImmediate");

        assertThat(inResult.size()).
                isEqualTo(nbRecords + nbImmDuplicates*nbRecords);

        // Deduplicate the stream
        Dataset<Row> outStream = deduplicateStream(inStream);

        // Collect the result
        List<Row> result = collectAsList(outStream, "OutputStreamImmediate");

        List<Row> expected = new ArrayList<>();

        IntStream
                .range(0, nbRecords)
                .forEach(i -> expected.add(RowFactory.create("key" + i, "value" + i)));

        assertThat(result.size()).
                isEqualTo(expected.size());

        assertThat(result)
                .containsExactlyInAnyOrder(expected.toArray(new Row[result.size()]));

    }


    private List<Row> collectAsList(Dataset<Row> stream, String output) {
        stream
                // Collect the result in memory for testing
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .writeStream()
                .format("memory")
                .queryName(output)
                .outputMode(Append())
                .start()

                .processAllAvailable();


        return spark.sql("select * from " + output).collectAsList();
    }

    private Dataset<Row> deduplicateStream(Dataset<Row> stream) {
        // Your deduplication logic here - Business code
        return stream.selectExpr("key", "value")
                // Dedublicate based on the hash of the value
                .withColumn("contentId", hash(col("value")))
                //.withWatermark("event_time", "15 minutes") => Add the event time to limit the search scope & prevent OOM Errors
                .dropDuplicates("contentId")
                .drop("contentId");
    }


    private Dataset<Row> createStreamingDataFrame(String topic) {

        return spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", getKafkaConnectString())
                .option("failOnDataLoss", "true") // Important
                .option("fetchOffset.numRetries", "3")
                .option("startingOffsets", "earliest")
                // 10 offsets per batch
                .option("maxOffsetsPerTrigger", "10")
                .option("kafkaConsumer.pollTimeoutMs", "1000")
                .option("subscribe", topic)
                .option("value.converter.schemas.enable", "false")
                .load();
    }

}
