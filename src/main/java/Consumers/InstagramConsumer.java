package Consumers;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.jinstagram.Instagram;
import org.jinstagram.auth.model.Token;
import org.jinstagram.entity.common.Location;
import org.jinstagram.entity.users.feed.MediaFeed;
import org.jinstagram.entity.users.feed.MediaFeedData;
import org.jinstagram.exceptions.InstagramException;
import org.lightcouch.CouchDbClient;
import org.lightcouch.NoDocumentException;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class InstagramConsumer extends Thread {

    private final Token token;
    private CouchDbClient dbClient;

    public InstagramConsumer(CouchDbClient dbClient, String secret, String token) {
        this.dbClient = dbClient;
        this.token = new Token(token, secret);
    }

    public void run() {

        while (true) {

            try {

                Instagram instagram = new Instagram(token);
                Random random = new Random();

                while (true) {

                    long dbLatest = getLatestDate();
//                  System.out.println("dblatest: "+dbLatest);
//                  dblatest = dbClient.view("_latest_instagram/_view").descending(true).limit(1).queryForLong();

                    double latitude = -37.8136;
                    double longitude = 144.9631;

                    MediaFeed feed = instagram.searchMedia(latitude, longitude, 5000);

                    List<MediaFeedData> list = feed.getData();
                    saveLatestInstagramDate(list.get(0));

                    for (MediaFeedData post : list) {
                        long createdTime = 1000 * Long.parseLong(post.getCreatedTime());
                        if (createdTime > dbLatest) {
                            saveToDB(post, createdTime);
                        }
                    }
                    //waits between 3-8 seconds, equation is nextInt(max-min+1)+min
                    TimeUnit.SECONDS.sleep(random.nextInt(6) + 3);
                }
            } catch (InstagramException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    private void saveToDB(MediaFeedData post, long createdTime) {
        Location location = post.getLocation();

        JsonArray coordinates = new JsonArray();
        JsonObject flickrObj = new JsonObject();

        flickrObj.addProperty("timestamp", createdTime);
        JsonPrimitive postLatitude = new JsonPrimitive(location.getLatitude());
        JsonPrimitive postLongitude = new JsonPrimitive(location.getLongitude());
        coordinates.add(postLatitude);
        coordinates.add(postLongitude);
        flickrObj.add("coordinates", coordinates);
        flickrObj.addProperty("instagram", Long.parseLong(post.getUser().getId()));
        System.out.println(flickrObj.toString());
        dbClient.save(flickrObj);
    }


    private long getLatestDate() {
        try {
            JsonObject latestDbObj = dbClient.find(JsonObject.class, "latest_instagram");
            return latestDbObj.get("latest_instagram").getAsLong();
        } catch (NoDocumentException e) {
            System.out.println("No latest_instagram document found, continuing");
            return 0;
        }
    }

    public void saveLatestInstagramDate(MediaFeedData post) {

        JsonObject latestTimeObj = new JsonObject();
        long createdTime = 1000 * Long.parseLong(post.getCreatedTime());
//        System.out.println(createdTime);

        try {
            JsonObject latestDbObj = dbClient.find(JsonObject.class, "latest_instagram");
            String latestRev = latestDbObj.get("_rev").getAsString();
            latestTimeObj.addProperty("_id", "latest_instagram");
            latestTimeObj.addProperty("_rev", latestRev);
            latestTimeObj.addProperty("latest_instagram", createdTime);
            dbClient.update(latestTimeObj);
        } catch (NoDocumentException e) {
            System.out.println("No latest_instagram document found, creating.");
            latestTimeObj.addProperty("_id", "latest_instagram");
            latestTimeObj.addProperty("latest_instagram", createdTime);
            dbClient.save(latestTimeObj);
        }
    }

}
