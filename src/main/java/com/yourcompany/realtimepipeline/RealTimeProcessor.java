package com.yourcompany.realtimepipeline;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.yourcompany.realtimepipeline.PostFilter.FilteredPost;

import java.util.*;

public class RealTimeProcessor {

    private static final String ACCESS_TOKEN = "EAATkvzZBSfbABO37YnZARAOZBD7SMW4MryMjJFBEC48k7V7S6uhXun7qlwWljZAzrBvDE4gD4S8HUlBy6uv85gAg57p4IexFJ22jaBA0ZBQDZBMan7yD7Fj8HfP87IctdGAVvax96FK5ZBnVMvHXBz4zzKYI36h5JMHSHcPKsemL4plzaYxif5zhvuxz3LARDtWn7oh7hpG1U00ZATs5oDLaLriY";  // Replace with a valid access token
    private static final List<String> KEYWORDS = Arrays.asList("#sports", "#news");  // Add your desired hashtags/keywords

    private static final Set<String> seenPostIds = new HashSet<>();
    private static final List<FilteredPost> matchedPosts = new ArrayList<>();
    private static final Map<String, Integer> keywordPostCount = new HashMap<>();  // Track post count by keyword

    public static void main(String[] args) {

        Timer timer = new Timer();

        // Adding shutdown hook to show summary on exit
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠");
            System.out.println("➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0FSummary Of Your PipeLine⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F");
            System.out.println("➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠\n");

            int totalWordCount = matchedPosts.stream().mapToInt(fp -> fp.wordCount).sum();
            for (String keyword : KEYWORDS) {
                int count = keywordPostCount.getOrDefault(keyword, 0);
                System.out.println("\uD83D\uDFE1Hashtag: " + keyword);
                System.out.println("✅Total posts: " + count);
                System.out.println("➠➠➠➠➠➠➠➠➠➠➠➠");
            }
            System.out.println("\uD83D\uDCA0Total word count across all posts: " + totalWordCount);
            KafkaPublisher.close(); // Close Kafka producer
        }));

        // Set up a timer to fetch new posts every 30 seconds
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                JsonArray rawPosts = FacebookDataIngestion.fetchFacebookPosts(ACCESS_TOKEN);
                if (rawPosts.size() == 0) {
                    System.out.println("⚠️ No posts fetched.");
                    return;
                }

                List<JsonObject> newPosts = new ArrayList<>();

                // Filter out already seen posts
                for (int i = 0; i < rawPosts.size(); i++) {
                    JsonObject post = rawPosts.get(i).getAsJsonObject();
                    if (!seenPostIds.contains(post.get("id").getAsString())) {
                        newPosts.add(post);
                        seenPostIds.add(post.get("id").getAsString());
                    }
                }

                if (!newPosts.isEmpty()) {
                    List<FilteredPost> filtered = PostFilter.filterPosts(newPosts, KEYWORDS);

                    // For each filtered post, update the keyword count and publish to Kafka
                    System.out.println("➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠");
                    System.out.println("➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F➡\uFE0F  Publish To Kafka Topic ⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F⬅\uFE0F");
                    System.out.println("➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠➠\n");
                    for (FilteredPost fp : filtered) {
                        matchedPosts.add(fp);

                        // Update the post count for the matched keyword
                        keywordPostCount.put(fp.matchedKeyword,
                                keywordPostCount.getOrDefault(fp.matchedKeyword, 0) + 1);

                        // Publish to Kafka
                        try {


                            KafkaPublisher.sendMessage(fp.toJson());
                            System.out.println("✅ Published post to Kafka: " + fp.matchedKeyword);
                            System.out.println("➠➠➠➠➠➠➠➠➠➠➠➠\n");
                        } catch (Exception e) {
                            System.err.println("🚨 Error publishing to Kafka: " + e.getMessage());
                        }
                    }
                }
            }
        }, 0, 5000);  // Fetch posts every 30 seconds
    }
}