package Consumers; /**
 * Created by geoffcunliffe on 1/12/2016.
 */

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.DateTime;
import com.google.api.client.util.Joiner;
import com.google.api.services.youtube.YouTube;
import com.google.api.services.youtube.model.GeoPoint;
import com.google.api.services.youtube.model.SearchListResponse;
import com.google.api.services.youtube.model.SearchResult;
import com.google.api.services.youtube.model.Video;
import com.google.api.services.youtube.model.VideoListResponse;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.lightcouch.CouchDbClient;

import java.io.IOException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class YoutubeConsumer extends Thread {

    private String apiKey;
    CouchDbClient dbClient;

    public YoutubeConsumer(CouchDbClient dbClient, String apiKey) {
        this.dbClient = dbClient;
        this.apiKey = apiKey;
    }

    public void run() {

        String location = "-38.2601,144.3537";
        String locationRadius = "50km";

        YouTube youtube = new YouTube.Builder(new NetHttpTransport(), new JacksonFactory(), new HttpRequestInitializer() {
//            @Override
            public void initialize(HttpRequest request) throws IOException {
            }
        }).setApplicationName("streamerater").build();

        // Define the API request for retrieving search results.
        YouTube.Search.List search = null;
        try {
            search = search = youtube.search().list("snippet");
        } catch (IOException e) {
            e.printStackTrace();
        }

        search.setKey(apiKey);
        search.setOrder("date");
        search.setLocation(location);
        search.setLocationRadius(locationRadius);
        search.setType("video");
        // As a best practice, only retrieve the fields that the application uses.
        search.setFields("items(id/videoId)");
        search.setMaxResults((long) 25);


        while (true) {

            try {

                SearchListResponse searchResponse = search.execute();
                List<SearchResult> searchResultList = searchResponse.getItems();
                List<String> videoIds = new ArrayList<String>();
                if (searchResultList != null) {

                    // Merge video IDs
                    for (SearchResult searchResult : searchResultList) {
                        videoIds.add(searchResult.getId().getVideoId());
                    }
                    Joiner stringJoiner = Joiner.on(',');
                    String videoId = stringJoiner.join(videoIds);

                    // Call the YouTube Data API's youtube.videos.list method to
                    // retrieve the resources that represent the specified videos.
                    YouTube.Videos.List listVideosRequest = youtube.videos().list("snippet, recordingDetails").setId(videoId);
                    listVideosRequest.setKey(apiKey);
                    VideoListResponse listResponse = listVideosRequest.execute();

                    List<Video> videoList = listResponse.getItems();

                    if (videoList.isEmpty())  {
                        System.out.println("ain't nothing there, man");
                        TimeUnit.SECONDS.sleep(30);
                    }
                    else {
                        DateTime latest = new DateTime(videoList.get(0).getSnippet().getPublishedAt().getValue() + 1000);
                        search.setPublishedAfter(latest);
                        saveToDB(videoList.iterator(), dbClient);
                    }

                }
            } catch (GoogleJsonResponseException e) {
                System.err.println("There was a service error: " + e.getDetails().getCode() + " : "
                        + e.getDetails().getMessage());
            } catch (IOException e) {
                System.err.println("There was an IO error: " + e.getCause() + " : " + e.getMessage());
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }


    private void saveToDB(Iterator<Video> iteratorVideoResults, CouchDbClient dbClient) {

        if (!iteratorVideoResults.hasNext()) {
            System.out.println(" There aren't any results for your query.");
        }

        while (iteratorVideoResults.hasNext()) {

            Video singleVideo = iteratorVideoResults.next();
            DateTime dateTaken = singleVideo.getRecordingDetails().getRecordingDate();
            if (dateTaken != null) {
                GeoPoint location = singleVideo.getRecordingDetails().getLocation();
                JsonArray coordinates = new JsonArray();
                JsonObject videoObj = new JsonObject();

                videoObj.addProperty("timestamp", dateTaken.getValue());
                JsonPrimitive videoLatitude = new JsonPrimitive(location.getLatitude());
                JsonPrimitive videoLongitude = new JsonPrimitive(location.getLongitude());
                coordinates.add(videoLatitude);
                coordinates.add(videoLongitude);
                videoObj.add("coordinates", coordinates);
                videoObj.addProperty("youtube", singleVideo.getId());
                System.out.println(videoObj.toString());
                dbClient.save(videoObj);

            }

        }
    }
}

