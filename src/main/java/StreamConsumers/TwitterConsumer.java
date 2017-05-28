package StreamConsumers;

import FieldCreators.CoordinatesCreator;
import FieldCreators.DBEntryConstructor;
import FieldCreators.DateArrayCreator;
import Geo.SuburbFinder;
import KeyHandlers.TwitterKeyHandler;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.Location.Coordinate;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.lightcouch.CouchDbClient;
import org.lightcouch.CouchDbProperties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Class TwitterConsumer employs Twitter's Java Hosebird Client (hbc)
 * to consume Twitter's Streaming API. Messages are pushed by Twitter as a stream,
 * rather than recurring queries.<p>
 * API : https://github.com/twitter/hbc
 */

public class TwitterConsumer extends Thread {

    private CouchDbClient dbClient;
    CouchDbProperties properties;
    private Coordinate southwest, northeast;
    private TwitterKeyHandler twitterKeyHandler;
    private String city;

    /**
     *  Constructs a consumer object, setting the location, database client, and retrieving the API key(s)
     * @param  KEYFILE the filename of the file containing all API keys
     *  @param  city the name of the city from which to query posts
     * @param  properties CouchDB client properties
     */
    public TwitterConsumer(String KEYFILE, String city, CouchDbProperties properties) {

        if (city.equals("melbourne")) {
            this.southwest = new Coordinate(144.9061, -37.8549);
            this.northeast = new Coordinate(145.0200, -37.7686);
        } else {
            this.southwest = new Coordinate(151.1551, -33.9064);
            this.northeast = new Coordinate(151.2689, -33.8201);
        }
        this.city = city;
        this.twitterKeyHandler = new TwitterKeyHandler(KEYFILE,city);
        this.dbClient = new CouchDbClient(properties);
        this.properties = properties;
    }

    /**
     *  Starts the consumer thread to retrieve latest posts
     */
    public void run() {

        // Create a blocking queue
        BlockingQueue<String> queue = new LinkedBlockingQueue<String>();

        // Define endpoint
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

        //Add Melbourne as the location to the endpoint using the passed in Polygon coordinates
        Location melbourne = new Location(southwest, northeast);
        endpoint.locations(Lists.newArrayList(melbourne));

        Authentication auth = new OAuth1(twitterKeyHandler.getTwitterConsumerKey(),
                twitterKeyHandler.getTwitterConsumerSecret(),
                twitterKeyHandler.getTwitterAccessTokenKey(),
                twitterKeyHandler.getTwitterAccessTokenSecret());

        // Create a new BasicClient.
        BasicClient client = new ClientBuilder()
                .name("Streamerater")
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        // Establish a connection
        client.connect();

        // Create a parser to parse the received msg
        JsonParser parser = new JsonParser();

        // Do what needs to be done with messages
        while (true) {
            if (client.isDone()) {
                System.out.println("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
                break;
            }

            //Retreive messages from the stream
            String msg = null;
            try {
                msg = queue.poll(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            //Parse the message as a Json object, process it and save the DB object to the database
            if (msg == null) {
//                System.out.println("Did not receive a message in 60 seconds");
            } else {

                JsonObject obj = (JsonObject) parser.parse(msg);

                if (obj.has("coordinates") && !obj.get("coordinates").isJsonNull()) {
                    DBEntryConstructor dbEntry = getFields(obj);
//                    System.out.println(dbEntry.getdbObject().toString());
                    dbClient.save(dbEntry.getdbObject());
                    TwitterUserQuerier userQuerier = new TwitterUserQuerier(twitterKeyHandler, city, properties, obj.getAsJsonObject("user"));
                    userQuerier.start();
                }
            }
        }

        client.stop();
        dbClient.shutdown();

    }
    //Method getFields extracts the required info from the passed in retrieved post
    private DBEntryConstructor getFields(JsonObject obj) {

        Long timestamp = Long.parseLong(obj.get("timestamp_ms").toString().replaceAll("\"", ""));
        String user = obj.getAsJsonObject("user").get("id").getAsString();
        String text = obj.get("text").toString();

        DateArrayCreator dateArrayCreator = new DateArrayCreator(timestamp);
        JsonArray dateArray = dateArrayCreator.getDateArray();

        JsonArray backwardsCoordinates = obj.getAsJsonObject("coordinates").get("coordinates").getAsJsonArray();
        JsonPrimitive latitude = backwardsCoordinates.get(1).getAsJsonPrimitive();
        JsonPrimitive longitude = backwardsCoordinates.get(0).getAsJsonPrimitive();
        CoordinatesCreator coordinatesCreator = new CoordinatesCreator(latitude,longitude);
        JsonArray coordinates = coordinatesCreator.getCoordinates();

        SuburbFinder suburbFinder = new SuburbFinder(latitude,longitude,city);
        String suburb = suburbFinder.getSuburb();

        return new DBEntryConstructor(timestamp,dateArray,coordinates,"twitter",user,text, suburb);

    }

}
