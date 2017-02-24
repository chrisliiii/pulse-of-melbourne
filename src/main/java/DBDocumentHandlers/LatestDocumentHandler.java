package DBDocumentHandlers;

import com.google.api.client.util.DateTime;
import com.google.gson.JsonObject;
import org.lightcouch.CouchDbClient;
import org.lightcouch.NoDocumentException;

/**
 * Class LatestDocumentHandler is used by the FlickrConsumer and Youtube Consumer classes to
 * save the post date of the newest retreived search result. A separate object is necessary
 * because the time normally saved with each document represents represents the photo or video,
 * while the most recent post date is required to ensure only the newest documents are returned
 * from a search query.
 */
public class LatestDocumentHandler {

    private CouchDbClient dbClient;

    public LatestDocumentHandler(CouchDbClient dbClient) {
        this.dbClient = dbClient;
    }

/**
 *  Method getLatestDate returns the DateTime of the newest post, querying the db for a
 *   document with an ID of the passed in id String.
 *   @param id The id of the CouchDB document to be retrieved
 *   @return The date of the latest post of a previous query, or date(0) if no document is found
 */
    public DateTime getLatestDate(String id) {
        try {
            JsonObject latestDbObj = this.dbClient.find(JsonObject.class, id);
            return new DateTime(latestDbObj.get(id).getAsLong());
        } catch (NoDocumentException e) {
            System.out.println("No " + id + " document found, continuing.");
            return new DateTime(0);
        }
    }

/**
 * Method saveLatestDate updates a DB document in the database with an ID of the passed in id String
 * to contain the most recent timestamp from the latest posted date from the youtube server.
 * This is then compared to new search results to only save the newest data.
 * @param id The id of the CouchDB document to be retrieved
 * @param timestamp The newest timestamp to replace the old one
 */
    public void saveLatestDate(String id, long timestamp) {
        JsonObject latestTimeObj = new JsonObject();
        try {
            JsonObject latestDbObj = this.dbClient.find(JsonObject.class, id);
            latestDbObj.remove(id);
            latestDbObj.addProperty(id, timestamp);
            this.dbClient.update(latestDbObj);
        } catch (NoDocumentException e) {
            System.out.println("No " + id + " document found, creating.");
            latestTimeObj.addProperty("_id", id);
            latestTimeObj.addProperty(id, timestamp);
            this.dbClient.save(latestTimeObj);
        }
    }



}
