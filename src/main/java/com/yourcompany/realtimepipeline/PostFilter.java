package com.yourcompany.realtimepipeline;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.util.List;
import java.util.ArrayList;

public class PostFilter {
    public static List<FilteredPost> filterPosts(List<JsonObject> posts, List<String> keywords) {
        List<FilteredPost> result = new ArrayList<>();

        for (JsonObject post : posts) {
            if (!post.has("message")) continue;
            String message = post.get("message").getAsString();

            for (String keyword : keywords) {
                if (message.contains(keyword)) {
                    int wordCount = message.trim().split("\\s+").length;
                    result.add(new FilteredPost(post, keyword, wordCount));
                    break; // avoid duplicates if multiple keywords
                }
            }
        }

        return result;
    }

    public static class FilteredPost {
        public JsonObject post;
        public String matchedKeyword;
        public int wordCount;

        public FilteredPost(JsonObject post, String keyword, int wordCount) {
            this.post = post;
            this.matchedKeyword = keyword;
            this.wordCount = wordCount;
        }

        // Serialize to JSON for Kafka
        public String toJson() {
            return new Gson().toJson(this);
        }
    }
}