package com.yourcompany.realtimepipeline;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

public class SparkKafkaConsumer
{
    // Track cumulative keyword post counts across all batches
    private static final Map<String, Integer> cumulativeKeywordCounts = new HashMap<>();

    public static void main(String[] args)
    {
        try {
            // Configure Spark
            SparkConf conf = new SparkConf()
                    .setAppName("KafkaSparkStreaming")
                    .setMaster("local[*]"); // Use local mode for testing
            JavaStreamingContext streamingContext =
                    new JavaStreamingContext(conf, Durations.seconds(30));

            // Kafka configuration
            Map<String, Object> kafkaParams = new HashMap<>();
            kafkaParams.put("bootstrap.servers", "localhost:9092");
            kafkaParams.put("key.deserializer", StringDeserializer.class.getName());
            kafkaParams.put("value.deserializer", StringDeserializer.class.getName());
            kafkaParams.put("group.id", "spark-consumer-group");
            kafkaParams.put("auto.offset.reset", "latest");
            kafkaParams.put("enable.auto.commit", true);

            // Kafka topic
            Collection<String> topics = Collections.singletonList("filtered-posts");

            // Create Kafka input stream
            JavaInputDStream<ConsumerRecord<String, String>> stream =
                    KafkaUtils.createDirectStream(
                            streamingContext,
                            LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.Subscribe(topics, kafkaParams)
                    );

            // Extract message values and deserialize JSON
            JavaDStream<JsonObject> messages = stream.map(record -> {
                try {
                    return new Gson().fromJson(record.value(), JsonObject.class);
                } catch (Exception e) {
                    System.err.println("🚨 Error deserializing JSON: " + record.value());
                    return new JsonObject();
                }
            });

            // Extract message text for word count
            JavaDStream<String> messageTexts = messages.map(json -> {
                if (json.has("post") && json.get("post").getAsJsonObject().has("message")) {
                    return json.get("post").getAsJsonObject().get("message").getAsString();
                }
                return "";
            });

            // Extract keywords for post count
            JavaDStream<String> keywords = messages.map(json -> {
                if (json.has("matchedKeyword")) {
                    return json.get("matchedKeyword").getAsString();
                }
                return "";
            });

            // Perform word count transformation
            JavaDStream<String> words = messageTexts.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());
            JavaPairDStream<String, Integer> wordCounts = words
                    .mapToPair(word -> new Tuple2<>(word, 1))
                    .reduceByKey((a, b) -> a + b);

            // Perform keyword post count transformation
            JavaPairDStream<String, Integer> keywordCounts = keywords
                    .mapToPair(keyword -> new Tuple2<>(keyword, 1))
                    .reduceByKey((a, b) -> a + b);

            // Print formatted results
            final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            final long[] batchCounter = {0};
            messages.foreachRDD(rdd -> {
                batchCounter[0]++;
                System.out.println("➠➠➠➠➠➠➠➠➠➠➠➠➠➠");
                System.out.println("⬇\uFE0F⬇\uFE0F⬇\uFE0F⬇\uFE0F⬇\uFE0F⬇\uFE0F⬇\uFE0F⬇\uFE0F⬇\uFE0F⬇\uFE0F");
                System.out.println("Batch of All old Posts");
                System.out.println("\uD83D\uDFE5 Batch #" + batchCounter[0] + " at " + sdf.format(new Date()));
                System.out.println("➠➠➠➠➠ New Batch of Posts ➠➠➠➠➠\n");
                rdd.foreach(json -> {
                    if (json.has("post") && json.has("matchedKeyword") && json.has("wordCount")) {
                        String message = json.get("post").getAsJsonObject().get("message").getAsString();
                        String keyword = json.get("matchedKeyword").getAsString();
                        int wordCount = json.get("wordCount").getAsInt();
                        System.out.println("\uD83D\uDD35 Message: " + message);
                        System.out.println("\uD83D\uDD35 Keyword: " + keyword);
                        System.out.println("\uD83D\uDD35 Word Count: " + wordCount);
                        System.out.println("➠➠➠➠➠➠➠➠➠➠➠➠➠➠\n");
                    }
                });
            });

            wordCounts.foreachRDD(rdd -> {
                System.out.println("➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠\n");
                System.out.println("\uD83D\uDFE5 Word Count Summary:\n");
                rdd.foreach(tuple -> System.out.println("Word: " + tuple._1 + "➠ Count: " + tuple._2));
                System.out.println("➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠");
            });

            keywordCounts.foreachRDD(rdd -> {
                System.out.println("\uD83D\uDFE5 Keyword Post Count Summary:\n");
                rdd.foreach(tuple -> {
                    String keyword = tuple._1;
                    int count = tuple._2;
                    if (!keyword.isEmpty()) {
                        // Update cumulative counts
                        cumulativeKeywordCounts.put(keyword,
                                cumulativeKeywordCounts.getOrDefault(keyword, 0) + count);
                        System.out.println("Keyword: " + keyword + ", Posts (this batch): " + count +
                                ", Total Posts: " + cumulativeKeywordCounts.get(keyword));
                    }
                });
                System.out.println("➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠");
                System.out.println("⬆\uFE0F⬆\uFE0F⬆\uFE0F⬆\uFE0F⬆\uFE0F⬆\uFE0F⬆\uFE0F⬆\uFE0F⬆\uFE0F⬆\uFE0F");

            });

            // Start streaming
            streamingContext.start();
            streamingContext.awaitTermination();
        } catch (Exception e) {
            System.err.println("🚨 Error in Spark Streaming: " + e.getMessage());
            e.printStackTrace();
        }
    }
}