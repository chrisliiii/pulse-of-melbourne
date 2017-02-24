package FieldCreators;

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;

/**
 * Class CoordinatesCreator creates a Json Array containing the latitude and longitude coordinates
 * of a social media post
 */
public class CoordinatesCreator {

    private JsonArray coordinates;

    /**
     * The CoordinatesCreator constuctor creates the coordinate array field to be saved with a db document
     * @param latitude the latitude coordinate of a social media post
     * @param longitude the longitude coordinate of a social media post
     */
    public CoordinatesCreator(JsonPrimitive latitude, JsonPrimitive longitude) {

        this.coordinates = new JsonArray();
        this.coordinates.add(latitude);
        this.coordinates.add(longitude);

    }

    public JsonArray getCoordinates() {
        return this.coordinates;
    }
}
