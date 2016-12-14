package Consumers; /**
 * Created by geoff on 24/03/2016.
 */
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
import org.json.JSONArray;
import org.lightcouch.CouchDbClient;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterConsumer extends Thread {

    private final String consumerKey;
    private final String consumerSecret;
    private final String token;
    private final String secret;
    private CouchDbClient dbClient;

    public TwitterConsumer(CouchDbClient dbClient, String consumerKey, String consumerSecret, String token, String secret) {
        this.dbClient = dbClient;
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.token = token;
        this.secret = secret;
    }

    public void run() {

        // Create an appropriately sized blocking queue
        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);

        // Define our endpoint: By default, delimited=length is set (we need this for our processor)
        // and stall warnings are on.
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        Coordinate southwest = new Coordinate(144.3537, -38.2601);
        Coordinate northeast = new Coordinate(145.3045, -37.3040);
        Location melbourne = new Location(southwest,northeast);
        endpoint.locations(Lists.newArrayList(melbourne));

        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
        //Authentication auth = new com.twitter.hbc.httpclient.auth.BasicAuth(username, password);

        // Create a new BasicClient. By default gzip is enabled.
        BasicClient client = new ClientBuilder()
                .name("Streamerater")
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        // Establish a connection
        client.connect();
        JsonParser parser = new JsonParser();

        // Do whatever needs to be done with messages
        for (int msgRead = 0; msgRead < 1000; msgRead++) {
            if (client.isDone()) {
                System.out.println("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
                break;
            }

            String msg = null;
            try {
                msg = queue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (msg == null) {
                System.out.println("Did not receive a message in 5 seconds");
            } else {
                JsonObject obj = (JsonObject) parser.parse(msg);
                if (obj.has("coordinates") && !obj.get("coordinates").isJsonNull()) {
                    JsonObject twitterObj = new JsonObject();
                    JsonArray coordinates = new JsonArray();

                    twitterObj.addProperty("timestamp",Long.parseLong(obj.get("timestamp_ms").toString().replaceAll("\"","")));

                    JsonElement backwardsCoordinates = obj.getAsJsonObject("coordinates").get("coordinates");
                    System.out.println(backwardsCoordinates);
//                    JsonPrimitive latitude = backwardsCoordinates.getAsJsonPrimitive("latitude");
//                    JsonPrimitive longitude = backwardsCoordinates.getAsJsonPrimitive("longitude");
//                    coordinates.add(latitude);
//                    coordinates.add(longitude);

                    twitterObj.add("coordinates",obj.getAsJsonObject("coordinates").get("coordinates"));
                    twitterObj.add("twitter",obj.getAsJsonObject("user").get("id"));
                    System.out.println(twitterObj.toString());
                    dbClient.save(twitterObj);
                }
            }
        }

        client.stop();
        dbClient.shutdown();

        // Print some stats
        System.out.printf("The client read %d messages!\n", client.getStatsTracker().getNumMessages());
    }

}
