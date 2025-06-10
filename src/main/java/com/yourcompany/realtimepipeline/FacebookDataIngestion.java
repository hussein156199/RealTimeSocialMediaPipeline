package com.yourcompany.realtimepipeline;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.client.HttpClient;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import java.io.IOException;

public class FacebookDataIngestion {
    private static final String API_URL = "https://graph.facebook.com/643134708878429/posts?access_token=";

    public static JsonArray fetchFacebookPosts(String accessToken) {
        try {
            HttpClient client = HttpClients.createDefault();
            HttpGet request = new HttpGet(API_URL + accessToken);
            HttpResponse response = client.execute(request);
            int statusCode = response.getStatusLine().getStatusCode();
            String responseBody = EntityUtils.toString(response.getEntity());

            if (statusCode != 200) {
                try {
                    JsonObject errorJson = new Gson().fromJson(responseBody, JsonObject.class);
                    String errorMessage = errorJson.has("error") ? errorJson.getAsJsonObject("error").get("message").getAsString() : "Unknown error";
                    System.err.println("🚨 Error fetching posts: HTTP " + statusCode + " - " + errorMessage);
                } catch (Exception e) {
                    System.err.println("🚨 Error fetching posts: HTTP " + statusCode + " - Response: " + responseBody);
                }
                return new JsonArray();
            }

            JsonObject jsonObject = new Gson().fromJson(responseBody, JsonObject.class);
            if (!jsonObject.has("data")) {
                System.err.println("🚨 Error: No 'data' field in API response");
                return new JsonArray();
            }
            return jsonObject.getAsJsonArray("data");
        } catch (IOException e) {
            System.err.println("🚨 Error fetching posts: " + e.getMessage());
            return new JsonArray();
        } catch (Exception e) {
            System.err.println("🚨 Unexpected error in fetchFacebookPosts: " + e.getMessage());
            return new JsonArray();
        }
    }
}