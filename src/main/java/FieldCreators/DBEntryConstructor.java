package FieldCreators;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * Class DBEntryConstructor constructs a Json object to be saved to CouchDB
 */
public class DBEntryConstructor {

    private JsonObject dbObject;

    /**
     * The DBEntryConstructor Constructor creates a Json object with all relevant document fields
     * @param timestamp The timestamp of a social media post
     * @param dateArray An array containing individual date details in the format
     *                  [YYYY,MM,DD,Hour,Minute,Second,DayOfWeek,Timezone]
     * @param coordinates the latitude and longitude coordinates of a social media post
     * @param type The social media platform time (Twitter, Instagram, etc)
     * @param user The user id associated to the entity
     * @param text Any text related to a social media post. Specifically:
     *             Twitter: tweet content
     *             Instagram: tags
     *             Flickr: title
     *             Foursquare: venue name
     *             Youtube: title
     * @param suburb
     */
    public DBEntryConstructor(long timestamp, JsonArray dateArray, JsonArray coordinates, String type, String user, String text, String suburb) {

        this.dbObject = new JsonObject();
        this.dbObject.addProperty("timestamp", timestamp);
        this.dbObject.add("date", dateArray);
        this.dbObject.add("coordinates", coordinates);
        this.dbObject.addProperty(type, user);
        this.dbObject.addProperty("text", text);
        this.dbObject.addProperty("suburb", suburb);
    }
    
    public JsonObject getdbObject() {
        return dbObject;
    }

}
