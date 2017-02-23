package FieldCreators;

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;

/**
 * Created by geoffcunliffe on 22/02/2017.
 */
public class CoordinatesCreator {

    private JsonArray coordinates;

    public CoordinatesCreator(JsonPrimitive latitude, JsonPrimitive longitude) {

        this.coordinates = new JsonArray();
        this.coordinates.add(latitude);
        this.coordinates.add(longitude);

    }

    public JsonArray getCoordinates() {
        return this.coordinates;
    }
}
