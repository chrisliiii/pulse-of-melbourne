package Consumers;

import FieldCreators.DateArrayCreator;
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
import org.lightcouch.NoDocumentException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class YoutubeConsumer extends Thread {

    private String apiKey, location;
    CouchDbClient dbClient;

    public YoutubeConsumer(CouchDbClient dbClient, String apiKey, String location) {
        this.dbClient = dbClient;
        this.apiKey = apiKey;
        this.location = location;
    }

    public void run() {

        while (true) {

            try {

                YouTube youtube = new YouTube.Builder(new NetHttpTransport(), new JacksonFactory(), new HttpRequestInitializer() {
                    public void initialize(HttpRequest request) throws IOException {}
                }).setApplicationName("streamerater").build();

                DateTime latestdate = getLatestDate();

                // Define the API request for retrieving search results.
                YouTube.Search.List search = youtube.search().list("snippet");
                search.setKey(apiKey);
                search.setOrder("date");
                search.setLocation(location);
                search.setLocationRadius("5km");
                search.setPublishedAfter(latestdate);
                search.setType("video");
                // As a best practice, only retrieve the fields that the application uses.
                search.setFields("items(id/videoId)");
                search.setMaxResults((long) 50);

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

                    if (videoList.isEmpty()) {
                        System.out.println("There aren't any results for your YOUTUBE query. Pausing 30 minutes");
                        TimeUnit.MINUTES.sleep(30);
                    } else {

                        DateTime latest = new DateTime(videoList.get(0).getSnippet().getPublishedAt().getValue() + 1000);
//                        System.out.println(latest.getValue());
                        saveLatestDate(latest);
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


    private DateTime getLatestDate() {
        try {
            JsonObject latestDbObj = dbClient.find(JsonObject.class, "latest_youtube");
            return new DateTime(latestDbObj.get("latest_youtube").getAsLong());
        } catch (NoDocumentException e) {
            System.out.println("No latest_youtube document found, continuing");
            return new DateTime(0);
        }
    }

    private void saveLatestDate(DateTime latest) {
        JsonObject latestTimeObj = new JsonObject();
//        System.out.println(latest.getValue());
        String id = "latest_youtube";
        try {
            JsonObject latestDbObj = dbClient.find(JsonObject.class, id);
            String latestRev = latestDbObj.get("_rev").getAsString();
            dbClient.remove(id, latestRev);
            latestTimeObj.addProperty("_id", id);
//            latestTimeObj.addProperty("_rev", latestRev);
            latestTimeObj.addProperty(id, latest.getValue());
            dbClient.save(latestTimeObj);
        } catch (NoDocumentException e) {
            System.out.println("No latest_youtube document found, creating.");
            latestTimeObj.addProperty("_id", id);
            latestTimeObj.addProperty(id, latest.getValue());
            dbClient.save(latestTimeObj);
        }
    }


    private void saveToDB(Iterator<Video> iteratorVideoResults, CouchDbClient dbClient) {

        if (!iteratorVideoResults.hasNext()) {
            System.out.println("There aren't any results for your YOUTUBE query.");
        }

        while (iteratorVideoResults.hasNext()) {

            Video singleVideo = iteratorVideoResults.next();
            DateTime dateTaken = singleVideo.getRecordingDetails().getRecordingDate();
            if (dateTaken != null) {

                GeoPoint location = singleVideo.getRecordingDetails().getLocation();
                JsonArray coordinates = new JsonArray();
                JsonObject videoObj = new JsonObject();

                DateArrayCreator dateArrayCreator = new DateArrayCreator(dateTaken.getValue());
                JsonArray dateArray = dateArrayCreator.getDateArray();

                JsonPrimitive videoLatitude = new JsonPrimitive(location.getLatitude());
                JsonPrimitive videoLongitude = new JsonPrimitive(location.getLongitude());
                coordinates.add(videoLatitude);
                coordinates.add(videoLongitude);

                videoObj.addProperty("timestamp", dateTaken.getValue());
                videoObj.add("date", dateArray);
                videoObj.add("coordinates", coordinates);
                videoObj.addProperty("youtube", singleVideo.getId());
                videoObj.addProperty("text", singleVideo.getSnippet().getTitle());
                System.out.println(videoObj.toString());
                dbClient.save(videoObj);

            }

        }
    }
}

