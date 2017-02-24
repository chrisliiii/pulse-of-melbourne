package StreamConsumers;

import DBDocumentHandlers.LatestDocumentHandler;
import FieldCreators.CoordinatesCreator;
import FieldCreators.DBEntryConstructor;
import FieldCreators.DateArrayCreator;
import KeyHandlers.YoutubeKeyHandler;
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
import com.google.gson.JsonPrimitive;
import org.lightcouch.CouchDbClient;
import org.lightcouch.CouchDbProperties;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Class YoutubeConsumer employs Google's Youtube Api to retrieve the most recent youtube
 * videos with geo data. A query is made to the StreamServer, getting the 50 latest videos
 * within a city within a 5km radius. After comparison to the most recent document
 * in the database, the newer posts are saved. Due to the presence os geo-tags being
 * uncommon on videos, there are seldom new results, and a query is done once every hour.<p>
 * API : https://developers.google.com/youtube/v3/
 */
public class YoutubeConsumer extends Thread {

    private String location;
    private CouchDbClient dbClient;
    private YoutubeKeyHandler youtubeKeyHandler;
    private final long MAXRESULTS = 50;
    YouTube youtube;

    /**
     *  Constructs a consumer object, setting the location, database client, and retrieving the API key(s)
     *  @param  KEYFILE the filename of the file containing all API keys
     *  @param  city the name of the city from which to query posts
     *  @param  properties CouchDB client properties
     */
    public YoutubeConsumer(String KEYFILE, String city, CouchDbProperties properties) {
        if (city.equals("melbourne")) {
            this.location = "-37.8136" + "," + "144.9631";
        } else {
            this.location = "-33.8688" + "," + "151.2093";
        }
        this.dbClient = new CouchDbClient(properties);
        youtubeKeyHandler = new YoutubeKeyHandler(KEYFILE, city);
    }

    /**
     *  Starts the consumer thread to retrieve latest posts
     */
    public void run() {

        try {

            //Create a new youtube query object
            youtube = new YouTube.Builder(new NetHttpTransport(),
                    new JacksonFactory(),
                    new HttpRequestInitializer() {
                public void initialize(HttpRequest request) throws IOException {
                }
            }).setApplicationName("streamerater").build();

            //Retreive the latest youtube document's timestamp from the DB
            LatestDocumentHandler latestDocumentHandler = new LatestDocumentHandler(dbClient);
            String latestID = "latest_youtube";
            DateTime latestdate = latestDocumentHandler.getLatestDate(latestID);

            // Define the API request for retrieving search results.
            YouTube.Search.List search = youtube.search().list("snippet");
            search.setKey(youtubeKeyHandler.getYoutubeApiKeyApiKey());
            search.setOrder("date");
            search.setLocation(location);
            search.setLocationRadius("5km");
            search.setPublishedAfter(latestdate); //Will only get results after the latest DB date
            search.setType("video");
            search.setFields("items(id/videoId)");
            search.setMaxResults(MAXRESULTS);

            //An initial query returns a simple results of video IDs
            SearchListResponse searchResponse = search.execute();
            List<SearchResult> searchResultList = searchResponse.getItems();
            List<String> videoIds = new ArrayList<String>();

            if (searchResultList != null) {

                // Merge video IDs so that a second query can retrieve more specific details about each video
                for (SearchResult searchResult : searchResultList) {
                    videoIds.add(searchResult.getId().getVideoId());
                }
                Joiner stringJoiner = Joiner.on(',');
                String videoId = stringJoiner.join(videoIds);

                // Call the YouTube Data API's youtube.videos.list method to
                // retrieve the resources that represent the specified videos.
                YouTube.Videos.List listVideosRequest = youtube.videos().list("snippet, recordingDetails").setId(videoId);
                listVideosRequest.setKey(youtubeKeyHandler.getYoutubeApiKeyApiKey());
                VideoListResponse listResponse = listVideosRequest.execute();

                List<Video> videoList = listResponse.getItems();

                if (videoList.isEmpty()) {
                    System.out.println("There aren't any results for your YOUTUBE query. Retrying in 1 Hour");

                } else {
                    //Save the newest date for future queries
                    DateTime latest = new DateTime(videoList.get(0).getSnippet().getPublishedAt().getValue() + 1000);
                    latestDocumentHandler.saveLatestDate(latestID,latest.getValue());

                    Iterator<Video> iteratorVideoResults = videoList.iterator();

                    //Save each new video that has a date
                    while (iteratorVideoResults.hasNext()) {
                        Video video = iteratorVideoResults.next();
                        DateTime dateTaken = video.getRecordingDetails().getRecordingDate();
                        if (dateTaken != null) {
                            DBEntryConstructor dbEntry = getFields(video, dateTaken);
//                            System.out.println(dbEntry.getdbObject().toString());
                            dbClient.save(dbEntry.getdbObject());
                        }
                    }
                }

            }
        } catch (GoogleJsonResponseException e) {
            System.err.println("There was a YouTube service error: " + e.getDetails().getCode() + " : "
                    + e.getDetails().getMessage());
        } catch (IOException e) {
            System.err.println("There was a YouTube IO error: " + e.getCause() + " : " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }

        dbClient.shutdown();

    }

    //Method getFields extracts the required info from the passed in video
    private DBEntryConstructor getFields(Video video, DateTime dateTaken) throws NullPointerException {
        long timestamp = dateTaken.getValue();

        DateArrayCreator dateArrayCreator = new DateArrayCreator(dateTaken.getValue());
        JsonArray dateArray = dateArrayCreator.getDateArray();

        GeoPoint location = video.getRecordingDetails().getLocation();
        JsonPrimitive latitude = new JsonPrimitive(location.getLatitude());
        JsonPrimitive longitude = new JsonPrimitive(location.getLongitude());
        CoordinatesCreator coordinatesCreator = new CoordinatesCreator(latitude, longitude);
        JsonArray coordinates = coordinatesCreator.getCoordinates();

        String user = video.getId();
        String text = video.getSnippet().getTitle();

        return new DBEntryConstructor(timestamp, dateArray, coordinates, "youtube", user, text);
    }
}

