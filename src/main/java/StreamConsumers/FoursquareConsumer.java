package StreamConsumers;

import FieldCreators.DBEntryConstructor;
import FieldCreators.CoordinatesCreator;
import FieldCreators.DateArrayCreator;
import KeyHandlers.FoursquareKeyHandler;
import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;
import fi.foyt.foursquare.api.FoursquareApi;
import fi.foyt.foursquare.api.FoursquareApiException;
import fi.foyt.foursquare.api.Result;
import fi.foyt.foursquare.api.entities.CompactVenue;
import fi.foyt.foursquare.api.entities.VenuesSearchResult;
import org.lightcouch.CouchDbClient;
import org.lightcouch.CouchDbProperties;

import java.util.HashSet;

/**
 * Class FoursquareConsumer employs AtlisInc's Foursquare Api
 * to query foursquare servers for current check-ins once every hour. A triangular mesh
 * is used over a 10km x 10km area, querying venues every 400 metres, while avoiding
 * duplicates by storing venue IDs in a hasSet. A single query cycle is completed in
 * approximately 15-20 minutes.<p>
 * API : https://github.com/AtlisInc/foursquare-api
 */

public class FoursquareConsumer extends Thread {

    private double lat, lon;
    private CouchDbClient dbClient;
    private FoursquareKeyHandler foursquareKeyHandler;

    /**
     *  Constructs a consumer object, setting the location, database client, and retrieving the API key(s)
     *  @param  KEYFILE the filename of the file containing all API keys
     *  @param  city the name of the city from which to query posts
     *  @param  properties CouchDB client properties
     */
    public FoursquareConsumer(String KEYFILE, String city, CouchDbProperties properties) {

        if (city.equals("melbourne")) {             //Melbourne
            this.lat = -37.768648;
            this.lon = 144.906196;
        } else {
            this.lat = -33.820142;
            this.lon = 151.155146;
        }
        this.foursquareKeyHandler = new FoursquareKeyHandler(KEYFILE);
        this.dbClient = new CouchDbClient(properties);
    }

    /**
     *  Starts the consumer thread to retrieve latest posts
     */
    public void run() {

        //Create a new foursquare object to query server
        FoursquareApi foursquareApi = new FoursquareApi(foursquareKeyHandler.getFoursquareClientID(),
                foursquareKeyHandler.getFoursquareClientSecret(),
                foursquareKeyHandler.getFoursquareCallbackUrl());

        //Sydney start position is -33.820142, 151.155146, end is -33.906449840000015,151.2689539999997
        //Melbourne start position is -37.768648, 144.906196, end is -37.85495584000002,145.0200039999997
        //400m offset
        double lonoffset = 0.00455232;
        double latoffset = 0.00359616;

        //This HashSet stores unique venue ids each query cycle to ensure no duplication
        HashSet<String> idSet = new HashSet<String>();

        // These for loops set up a rectangular mesh over a 10km*10km area, checking the venues
        // every 400 metres to find the number of people currently at them
        for (int i = 0; i < 25; i++) {
            //If the latitude value is on an even iteration, shift the longitude start over 400m
            if (i % 2 != 0) lon = lon - 0.00227616;

            for (int j = 0; j < 25; j++) {
                lon = lon + lonoffset;

                //ll = lat/lon string used for query parameters
                String ll = Double.toString(lat) + "," + Double.toString(lon);

                try {
                    //returns the closest 50 venues within a 230m radius. A 230m radius allows a small overlap due to the triangular mesh
                    Result<VenuesSearchResult> result = foursquareApi.venuesSearch(ll, null, null, null, null, 50, null, null, null, null, null, 230, null);

                    assert result != null;
                    if (result.getMeta().getCode() == 200) {
                        for (CompactVenue venue : result.getResult().getVenues()) {
                            long hereNowCount = venue.getHereNow().getCount();
                            String venueId = venue.getId();
                            //If there are current checkins and this venue hasn't been encountered yet, save the number of checkins to the DB
                            if (hereNowCount > 0 && !idSet.contains(venueId)) {
                                DBEntryConstructor dbEntry = getFields(venue);
//                                System.out.println(dbEntry.getdbObject().toString());
                                for (int count = 0; count < hereNowCount; count++) {
                                    dbClient.save(dbEntry.getdbObject());
                                }
                            }
                            idSet.add(venueId);
                        }
                    } else {
                        System.out.println("Error occured: ");
                        System.out.println("  code: " + result.getMeta().getCode());
                        System.out.println("  type: " + result.getMeta().getErrorType());
                        System.out.println("  detail: " + result.getMeta().getErrorDetail());
                    }

                } catch (FoursquareApiException e) {
                    e.printStackTrace();
                }
            }
            lat = lat - latoffset;
        }

        dbClient.shutdown();
    }

    // Method getFields extracts the required info from the passed in venue
    private DBEntryConstructor getFields(CompactVenue venue) {
        long timestamp = System.currentTimeMillis();
        DateArrayCreator dateArrayCreator = new DateArrayCreator(System.currentTimeMillis());
        JsonArray dateArray = dateArrayCreator.getDateArray();

        JsonPrimitive latitude = new JsonPrimitive(venue.getLocation().getLat());
        JsonPrimitive longitude = new JsonPrimitive(venue.getLocation().getLng());
        CoordinatesCreator coordinatesCreator = new CoordinatesCreator(latitude, longitude);
        JsonArray coordinates = coordinatesCreator.getCoordinates();

        String user = venue.getId();
        String text = venue.getName();

        return new DBEntryConstructor(timestamp, dateArray, coordinates, "foursquare", user, text);
    }


}
