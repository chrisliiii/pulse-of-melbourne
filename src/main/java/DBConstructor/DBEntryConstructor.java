package DBConstructor;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * Created by geoffcunliffe on 17/02/2017.
 *
 * Constructs a Json object to be saved to CouchDB
 */
public class DBEntryConstructor {

    private JsonObject dbObject = new JsonObject();

    public DBEntryConstructor(long timestamp, JsonArray dateArray, JsonArray coordinates, String type, String user, String text) {

        dbObject.addProperty("timestamp", timestamp);
        dbObject.add("date", dateArray);
        dbObject.add("coordinates", coordinates);
        dbObject.addProperty(type, user);
        dbObject.addProperty("text", text);

    }
    
    public JsonObject getdbObject() {
        return dbObject;
    }

}
