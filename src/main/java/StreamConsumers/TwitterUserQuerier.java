package StreamConsumers;

import FieldCreators.*;
import KeyHandlers.TwitterKeyHandler;
import com.google.gson.JsonObject;
import org.lightcouch.CouchDbClient;
import org.lightcouch.CouchDbProperties;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import java.util.ArrayList;
import java.util.List;

/**
 * Class TwitterUserQuerier employs Twitter4J to perform queries on a users timeline,
 * returning at most 3200 tweets. <p>
 * API : http://twitter4j.org/en/
 */
public class TwitterUserQuerier extends Thread{

    private CouchDbClient userdbClient;
    private CouchDbClient dbClient;
    private Twitter twitter;
    private String city;
    private JsonObject user;

    /**
     *  Constructs a TwitterUserQuerier object, setting the location, database client, and retrieving the API key(s)
     * @param  twitterKeyHandler a key handler containing all API keys
     *  @param  city the name of the city from which to query posts
     * @param  properties CouchDB client properties
     * @param user The user whose tweets will be retreived
     */
    public TwitterUserQuerier(TwitterKeyHandler twitterKeyHandler, String city, CouchDbProperties properties, JsonObject user) {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setOAuthConsumerKey(twitterKeyHandler.getTwitterConsumerKey());
        cb.setOAuthConsumerSecret(twitterKeyHandler.getTwitterConsumerSecret());
        cb.setOAuthAccessToken(twitterKeyHandler.getTwitterAccessTokenKey());
        cb.setOAuthAccessTokenSecret(twitterKeyHandler.getTwitterAccessTokenSecret());
        twitter = new TwitterFactory(cb.build()).getInstance();
        this.user = user;
        this.city = city;
        this.dbClient = new CouchDbClient(properties);
        properties.setDbName("twitter_users");
        this.userdbClient = new CouchDbClient(properties);
    }

    /**
     *  Starts the TwitterUserQuerier thread to retrieve tweets from a user
     */
    public void run() {
        long user_id = user.get("id").getAsLong();
        if (!userdbClient.contains(String.valueOf(user_id))) {
            user.addProperty("_id", String.valueOf(user_id));
            userdbClient.save(user);
            for (JsonObject object : retreiveTweets(user_id)) {
                if (!dbClient.contains(object.get("_id").getAsString()))
                    dbClient.save(object);
            }
        }
    }

    //Gets the tweets from a user posted after 20 Dec, 2016, up to 3200, as a List of Json objects
    private List<JsonObject> retreiveTweets(long id) {

        int pageno = 1;
        List<Status> statuses = new ArrayList();
        List<JsonObject> db_objects = new ArrayList<JsonObject>();

        while (true) {

            try {
                int size = statuses.size();
                Paging page = new Paging(pageno++, 200);
                //Limits returned results to those only after Dec 20, 2016
                page.setSinceId(Long.parseLong("810107288069357569"));
                statuses.addAll(twitter.getUserTimeline(id, page));
                if (statuses.size() == size)
                    break;
            }
            catch(TwitterException e) {
                e.printStackTrace();
            }
        }

        for (Status tweet : statuses) {
            if (tweet.getGeoLocation() != null) {
                DBEntryConstructor dbEntryConstructor = new Twitter4JStatusExtractor().getFields(tweet,city);
                JsonObject entry = dbEntryConstructor.getdbObject();
                entry.addProperty("_id", String.valueOf(tweet.getId()));
                db_objects.add(entry);
            }
        }
        return db_objects;
    }

}
