package FieldCreators;

import Geo.SuburbFinder;
import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;
import twitter4j.Status;

/**
 * Class Twitter4JStatusExtractor extracts information from a Twitter4J Status and returns
 * a database object in Json format
 */
public class Twitter4JStatusExtractor {

    /**
     * Method getFields extracts the required info from the passed in Twitter4J Status
     * @param obj A Twitter4J status (a tweet)
     * @param city the city location of the twitter stream query.
     */
    public DBEntryConstructor getFields(Status obj, String city) {

        Long timestamp = obj.getCreatedAt().getTime();
        String user = String.valueOf(obj.getUser().getId());
        String text = obj.getText();

        DateArrayCreator dateArrayCreator = new DateArrayCreator(timestamp);
        JsonArray dateArray = dateArrayCreator.getDateArray();

        JsonPrimitive latitude = new JsonPrimitive(obj.getGeoLocation().getLatitude());
        JsonPrimitive longitude = new JsonPrimitive(obj.getGeoLocation().getLongitude());
        CoordinatesCreator coordinatesCreator = new CoordinatesCreator(latitude,longitude);
        JsonArray coordinates = coordinatesCreator.getCoordinates();

        SuburbFinder suburbFinder = new SuburbFinder(latitude,longitude,city);
        String suburb = suburbFinder.getSuburb();

        return new DBEntryConstructor(timestamp,dateArray,coordinates,"twitter",user,text,suburb);

    }

}
