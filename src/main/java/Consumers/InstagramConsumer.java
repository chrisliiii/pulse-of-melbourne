package Consumers;

import FieldCreators.DBEntryConstructor;
import FieldCreators.CoordinatesCreator;
import FieldCreators.DateArrayCreator;
import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;
import org.jinstagram.Instagram;
import org.jinstagram.auth.model.Token;
import org.jinstagram.entity.common.Location;
import org.jinstagram.entity.users.feed.MediaFeed;
import org.jinstagram.entity.users.feed.MediaFeedData;
import org.jinstagram.exceptions.InstagramException;
import org.lightcouch.CouchDbClient;
import org.lightcouch.NoDocumentException;

import java.text.ParseException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by geoffcunliffe on 17/12/2017.
 *
 * class InstagramConsumer employs Sachin Handiekar's jinstagram Api
 * to query Instagram servers for the most recent posts. A query is made to
 * the Server, getting the 20 latest posts in a 5km radius from the city centre.
 * After comparison to the most recent document in the database, the newer posts
 * are saved.
 * API : https://github.com/sachin-handiekar/jInstagram
 */

public class InstagramConsumer extends Thread {

    private CouchDbClient dbClient;
    double latitude, longitude;

    public InstagramConsumer(CouchDbClient dbClient, String centerLatitude, String centerLongitude) {
        this.dbClient = dbClient;
        this.latitude = Double.parseDouble(centerLatitude);
        this.longitude = Double.parseDouble(centerLongitude);
    }

    public void run() {

        //Sets the tokens to be rotated for queries
        String[] tokens = new String[3];
        String[] secrets = new String[3];

        tokens[0] = "1447623382.e029fea.74bded2cf2a549899d9838a4b81ad63f";
        tokens[1] = "4281114253.e029fea.2fde1b2df2994f76a73196e9534e6917";
        tokens[2] = "4280694870.e029fea.392feabf42114199995a02eaad8c4cd8";

        secrets[0] = "46e8510efdb94050b9528ce92be5e77f";
        secrets[1] = "a5e48d8e3466442b9b8e80e2f1e8a0a2";
        secrets[2] = "f2e5a8ded2ac4bf79b721d7e140bb5ca";


        while (true) {

            try {

                Random random = new Random();

                //Chooses a random token to be used to access the Instagram servers. This rotates the keys
                //to avoid detection.
                int num = random.nextInt(3);
                Token token = new Token(tokens[num],secrets[num]);

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
                        System.out.println(dbEntry.getdbObject().toString());
                        dbClient.save(dbEntry.getdbObject());
                    }
                }

                //In order waits between 5-13 seconds, equation is nextInt(max-min+1)+min
                TimeUnit.SECONDS.sleep(random.nextInt(8) + 5);

            } catch (InstagramException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }catch (IllegalStateException e) {
                //These errors are fairly frequent due to a known bug with Instagram server-side
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

        String user = post.getUser().getId();
        String text = post.getTags().toString();

        return new DBEntryConstructor(timestamp,dateArray,coordinates,"instagram",user,text);
    }

}
