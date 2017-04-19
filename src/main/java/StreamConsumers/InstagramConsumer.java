package StreamConsumers;

import FieldCreators.DBEntryConstructor;
import FieldCreators.CoordinatesCreator;
import FieldCreators.DateArrayCreator;
import Geo.SuburbFinder;
import KeyHandlers.InstagramKeyHandler;
import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;
import org.jinstagram.Instagram;
import org.jinstagram.auth.model.Token;
import org.jinstagram.entity.common.Location;
import org.jinstagram.entity.users.feed.MediaFeed;
import org.jinstagram.entity.users.feed.MediaFeedData;
import org.jinstagram.exceptions.InstagramException;
import org.lightcouch.CouchDbClient;
import org.lightcouch.CouchDbProperties;
import org.lightcouch.NoDocumentException;

import java.text.ParseException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Class InstagramConsumer employs Sachin Handiekar's jinstagram Api
 * to query Instagram servers for the most recent posts. A query is made to
 * the StreamServer, getting the 20 latest posts in a 5km radius from the city centre.
 * After comparison to the most recent document in the database, the newer posts
 * are saved. <p>
 * API : https://github.com/sachin-handiekar/jInstagram
 */

public class InstagramConsumer extends Thread {

    private CouchDbClient dbClient;
    private double latitude, longitude;
    private InstagramKeyHandler instagramKeyHandler;
    private String city;

    /**
     *  Constructs a consumer object, setting the location, database client, and retrieving the API key(s)
     * @param  KEYFILE the filename of the file containing all API keys
     *  @param  city the name of the city from which to query posts
     * @param  properties CouchDB client properties
     */
    public InstagramConsumer(String KEYFILE, String city, CouchDbProperties properties) {
        if (city.equals("melbourne")) {
            this.latitude = -37.8136;
            this.longitude = 144.9631;
        } else {
            this.latitude = -33.8688;
            this.longitude = 151.2093;
        }
        this.city = city;
        this.dbClient = new CouchDbClient(properties);
        instagramKeyHandler = new InstagramKeyHandler(KEYFILE);
    }

    /**
     *  Starts the consumer thread to retrieve latest posts
     */
    public void run() {

        while (true) {

            try {

                Random random = new Random();

                //Chooses a random token to be used to access the Instagram servers. This rotates the keys
                //to avoid detection.
                int num = random.nextInt(3);
                Token token = new Token(instagramKeyHandler.getTokens()[num],
                        instagramKeyHandler.getSecrets()[num]);

                //Create a new instagram object to initiate queries
                Instagram instagram = new Instagram(token);

                //Retrieve the latest DB instagram document for comparison
                long dbLatest = getLatestViewDate();

                //Query instagram for the latest posts
                MediaFeed feed = instagram.searchMedia(latitude, longitude, 5000);
                List<MediaFeedData> list = feed.getData();

                //Iterates through the results and saves the newest posts
                for (MediaFeedData post : list) {
                    long createdTime = 1000 * Long.parseLong(post.getCreatedTime());
                    if (createdTime > dbLatest) {
                        DBEntryConstructor dbEntry = getFields(post, createdTime);
//                        System.out.println(dbEntry.getdbObject().toString());
                        dbClient.save(dbEntry.getdbObject());
                    }
                }

                // In order for access to appear random, the thread waits between 5-10 seconds at the end of
                // each while loop iteration. Equation is nextInt(max-min+1)+min
                TimeUnit.SECONDS.sleep(random.nextInt(5) + 5);

            } catch (InstagramException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }catch (IllegalStateException e) {
                //These errors are fairly frequent due to a known bug at Instagram server-side
                System.out.println("State exception, Error while reading response body");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    //Method getLatestViewDate uses a Couchdb view to retreive the timestamp of the most recent instagram post
    private long getLatestViewDate() {
        try {
            return dbClient.view("latest_instagram/_view").descending(true).limit(1).queryForLong();
        } catch (NoDocumentException e) {
            System.out.println("Unable to retrieve the latest timestamp, has the view been created? Continuing...");
            return 0;
        }
    }

    //Method getFields extracts the required info from the passed in retrieved post
    private DBEntryConstructor getFields(MediaFeedData post, long timestamp) throws ParseException {

        DateArrayCreator dateArrayCreator = new DateArrayCreator(timestamp);
        JsonArray dateArray = dateArrayCreator.getDateArray();

        Location location = post.getLocation();
        JsonPrimitive latitude = new JsonPrimitive(location.getLatitude());
        JsonPrimitive longitude = new JsonPrimitive(location.getLongitude());
        CoordinatesCreator coordinatesCreator = new CoordinatesCreator(latitude,longitude);
        JsonArray coordinates = coordinatesCreator.getCoordinates();

        SuburbFinder suburbFinder = new SuburbFinder(latitude,longitude, city);
        String suburb = suburbFinder.getSuburb();

        String user = post.getUser().getId();
        String text = post.getTags().toString();

        return new DBEntryConstructor(timestamp,dateArray,coordinates,"instagram",user,text, suburb);
    }

}
