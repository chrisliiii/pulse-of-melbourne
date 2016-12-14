package Consumers;

import com.flickr4java.flickr.photos.Photo;
import com.flickr4java.flickr.photos.PhotoList;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.jinstagram.Instagram;
import org.jinstagram.auth.InstagramAuthService;
import org.jinstagram.auth.model.Token;
import org.jinstagram.auth.model.Verifier;
import org.jinstagram.auth.oauth.InstagramService;
import org.jinstagram.entity.common.Location;
import org.jinstagram.entity.locations.LocationSearchFeed;
import org.jinstagram.entity.users.feed.MediaFeed;
import org.jinstagram.entity.users.feed.MediaFeedData;
import org.jinstagram.exceptions.InstagramException;
import org.lightcouch.CouchDbClient;
import org.lightcouch.CouchDbProperties;

import java.security.Timestamp;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import java.util.concurrent.TimeUnit;

/**
 * Created by geoffcunliffe on 20/11/2016.
 */
public class InstagramConsumer extends Thread {

    private final String apikey;
    private final String secret;
    private final String callbackUrl;
    private final Token token;
    private CouchDbClient dbClient;

    public InstagramConsumer(CouchDbClient dbClient, String apikey, String secret, String callbackUrl, String token) {
        this.dbClient = dbClient;
        this.apikey = apikey;
        this.secret = secret;
        this.callbackUrl = callbackUrl;
        this.token = new Token(token, secret);
    }

    public void run() {

        Instagram instagram = new Instagram(token);

        String locationId = "481504523";
//        Date minTime = new Date(1479627967);
        Date minTime = null;

        while (true) {

            try {
                double latitude = -37.8136;
                double longitude = 144.9631;

                MediaFeed feed = instagram.searchMedia(latitude, longitude, null, minTime, 5000);
//                MediaFeed feed = instagram.getRecentMediaByLocation(locationId);

                List<MediaFeedData> list = feed.getData();

                if (list.isEmpty()) {
                    System.out.println("No more posts for the time being, retrying in 30 seconds");
                    TimeUnit.SECONDS.sleep(30);
                }
                else minTime = new Date(Long.parseLong(list.get(10).getCreatedTime()));
//                Sun Jan 18 13:33:32 AEST 1970

                System.out.println(minTime.getTime());

                for (MediaFeedData post : list) {

                    Location location = post.getLocation();

                    JsonArray coordinates = new JsonArray();
                    JsonObject flickrObj = new JsonObject();

                    flickrObj.addProperty("timestamp",1000*Long.parseLong(post.getCreatedTime()));
                    JsonPrimitive postLatitude = new JsonPrimitive(location.getLatitude());
                    JsonPrimitive postLongitude = new JsonPrimitive(location.getLongitude());
                    coordinates.add(postLatitude);
                    coordinates.add(postLongitude);
                    flickrObj.add("coordinates", coordinates);
                    flickrObj.addProperty("instagram", Long.parseLong(post.getUser().getId()));
                    System.out.println(flickrObj.toString());
                    dbClient.save(flickrObj);
                }

            } catch (InstagramException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
