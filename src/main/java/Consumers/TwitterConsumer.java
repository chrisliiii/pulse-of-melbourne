package Consumers;

import FieldCreators.DBEntryConstructor;
import FieldCreators.CoordinatesCreator;
import FieldCreators.DateArrayCreator;
import com.google.common.collect.Lists;
import com.google.gson.*;
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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by geoffcunliffe on 17/02/2017.
 *
 * class TwitterConsumer employs Twitter's Java Hosebird Client (hbc)
 * to consume Twitter's Streaming API. Messages are pushed by Twitter as a stream,
 * rather than recurring queries.
 * API : https://github.com/twitter/hbc
 */

public class TwitterConsumer extends Thread {

    private final String consumerKey;
    private final String consumerSecret;
    private final String token;
    private final String secret;
    private final Coordinate southwest, northeast;
    private CouchDbClient dbClient;

    //Constructor to assign appropriate Keys, Tokens, and Location
    public TwitterConsumer(CouchDbClient dbClient, String consumerKey, String consumerSecret, String token, String secret, Coordinate southwest, Coordinate northeast) {
        this.dbClient = dbClient;
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.token = token;
        this.secret = secret;
        this.southwest = southwest;
        this.northeast = northeast;
    }

    public void run() {

        // Create an appropriately sized blocking queue
        BlockingQueue<String> queue = new LinkedBlockingQueue<String>();

        // Define endpoint
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

        //Add Melbourne as the location to the endpoint using the passed in Polygon coordinates
        Location melbourne = new Location(southwest, northeast);
        endpoint.locations(Lists.newArrayList(melbourne));

        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);

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
                System.out.println("Did not receive a message in 60 seconds");
            } else {

                JsonObject obj = (JsonObject) parser.parse(msg);

                if (obj.has("coordinates") && !obj.get("coordinates").isJsonNull()) {
                    DBEntryConstructor dbEntry = getFields(obj);
                    System.out.println(dbEntry.getdbObject().toString());
                    dbClient.save(dbEntry.getdbObject());
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

        return new DBEntryConstructor(timestamp,dateArray,coordinates,"twitter",user,text);

    }

}
