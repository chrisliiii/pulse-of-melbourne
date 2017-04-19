package StreamConsumers;

import FieldCreators.*;
import Geo.BBox;
import KeyHandlers.TwitterKeyHandler;
import com.google.gson.*;
import org.lightcouch.CouchDbClient;
import org.lightcouch.CouchDbProperties;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Class TwitterQuerier employs Twitter4J to perform queries on a location, given
 * latitude and longitude coordinates. Up to 100 tweets are retreived, every
 * 30 seconds. <p>
 * API : http://twitter4j.org/en/
 */
public class TwitterQuerier extends Thread{

    private final double latitude, longitude;
    private TwitterKeyHandler twitterKeyHandler;
    private CouchDbProperties properties;
    private CouchDbClient dbClient;
    private Twitter twitter;
    private String city;

    /**
     *  Constructs a TwitterQuerier object, setting the location, database client, and retrieving the API key(s)
     * @param  KEYFILE the filename of the file containing all API keys
     *  @param  city the name of the city from which to query posts
     * @param  properties CouchDB client properties
     */
    public TwitterQuerier(String KEYFILE, String city, CouchDbProperties properties) {
        this.twitterKeyHandler = new TwitterKeyHandler(KEYFILE, city);
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setOAuthConsumerKey(twitterKeyHandler.getTwitterConsumerKey());
        cb.setOAuthConsumerSecret(twitterKeyHandler.getTwitterConsumerSecret());
        cb.setOAuthAccessToken(twitterKeyHandler.getTwitterAccessTokenKey());
        cb.setOAuthAccessTokenSecret(twitterKeyHandler.getTwitterAccessTokenSecret());
        cb.setJSONStoreEnabled(true);
        this.properties = properties;
        twitter = new TwitterFactory(cb.build()).getInstance();
        this.city = city;
        this.dbClient = new CouchDbClient(properties);
        if (city.equals("melbourne")) {
            this.latitude = -37.8136;
            this.longitude = 144.9631;
        } else {
            this.latitude = -33.8688;
            this.longitude = 151.2093;
        }
    }

    /**
     *  Starts the TwitterQuerier thread to retrieve latest posts from a location
     */
    public void run() {

        long newest = 0;
        BBox box = new BBox(city);

        while (true) {

            List<JsonObject> db_objects = new ArrayList<JsonObject>();

            Query query = new Query().geoCode(new GeoLocation(latitude, longitude), 10, "km");
            query.setCount(100);
            query.setSinceId(newest);

            try {
                QueryResult result = twitter.search(query);

                for (Status tweet : result.getTweets()) {
                    if (tweet.getId() > newest) {
                        newest = tweet.getId();
                    }
                    if (tweet.getGeoLocation() != null && box.checkPoint(tweet.getGeoLocation().getLongitude(),tweet.getGeoLocation().getLatitude())) {
                        DBEntryConstructor dbEntryConstructor = new Twitter4JStatusExtractor().getFields(tweet,city);
                        JsonObject entry = dbEntryConstructor.getdbObject();
                        entry.addProperty("_id", String.valueOf(tweet.getId()));
                        db_objects.add(entry);
                        //Convert from a Twitter4j Status to a json object to use for the user search
                        String statusJson = DataObjectFactory.getRawJSON(tweet);
                        JsonElement jelement = new JsonParser().parse(statusJson);
                        JsonObject jobject = jelement.getAsJsonObject();
                        TwitterUserQuerier userQuerier = new TwitterUserQuerier(twitterKeyHandler, city, properties, jobject.getAsJsonObject("user"));
                        userQuerier.start();
                    }
                }

                for (JsonObject object : db_objects) {
                    if (!dbClient.contains(object.get("_id").getAsString()))
                        dbClient.save(object);
                }

                TimeUnit.SECONDS.sleep(30);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
