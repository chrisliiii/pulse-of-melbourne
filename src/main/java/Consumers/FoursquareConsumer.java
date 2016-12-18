package Consumers;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import fi.foyt.foursquare.api.FoursquareApi;
import fi.foyt.foursquare.api.FoursquareApiException;
import fi.foyt.foursquare.api.Result;
import fi.foyt.foursquare.api.entities.CompactVenue;
import fi.foyt.foursquare.api.entities.VenuesSearchResult;
import org.lightcouch.CouchDbClient;

import java.util.HashSet;

public class FoursquareConsumer extends Thread {

    private final String clientId;
    private final String secret;
    private final String callbackUrl;
    private CouchDbClient dbClient;

    public FoursquareConsumer(CouchDbClient dbClient, String clientId, String secret, String callbackUrl) {
        this.dbClient = dbClient;
        this.clientId = clientId;
        this.secret = secret;
        this.callbackUrl = callbackUrl;
    }

    public void run() {

        FoursquareApi foursquareApi = new FoursquareApi(clientId, secret, callbackUrl);
        long startTime = System.currentTimeMillis();

//
        //Sydney start position is -33.820142, 151.155146, end is -33.906449840000015,151.2689539999997
        //Melbourne start position is -37.768648, 144.906196, end is -37.85495584000002,145.0200039999997
//        double lat = -37.768648;
        double lat = -33.820142;
        double lon;

        //400m offset
        double lonoffset = 0.00455232;
        double latoffset = 0.00359616;

        HashSet<String> idSet = new HashSet<String>();

        // These for loops set up a rectangular mesh over a 10km*10km area, checking the venues
        // every 400 metres to find the number of people currently at them
        for (int i = 0; i < 25; i++) {

            //Melbourne
//            if (i % 2 == 0) lon = 144.906196;
//            else lon = 144.906196 - 0.00227616;
            //Sydney
            if (i % 2 == 0) lon = 151.155146;
            else lon = 151.155146 - 0.00227616;

            for (int j = 0; j < 25; j++) {
                lon = lon + lonoffset;

                String ll = Double.toString(lat) + "," + Double.toString(lon);
//                System.out.println(ll);

                try {
                    Result<VenuesSearchResult> result = foursquareApi.venuesSearch(ll, null, null, null, null, 50, null, null, null, null, null, 230, null);

                    assert result != null;
                    if (result.getMeta().getCode() == 200) {
                        // if query was ok we can finally we do something with the data
                        for (CompactVenue venue : result.getResult().getVenues()) {
                            long hereNowCount = venue.getHereNow().getCount();
                            String venueId = venue.getId();
                            if (hereNowCount > 0 && !idSet.contains(venueId)) {
                                saveToDB(venue, hereNowCount);
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
        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println(estimatedTime);
    }
    private void saveToDB(CompactVenue venue, long hereNowCount) {
        JsonArray coordinates = new JsonArray();
        JsonObject venueObj = new JsonObject();

        venueObj.addProperty("timestamp", System.currentTimeMillis());
        JsonPrimitive postLatitude = new JsonPrimitive(venue.getLocation().getLat());
        JsonPrimitive postLongitude = new JsonPrimitive(venue.getLocation().getLng());
        coordinates.add(postLatitude);
        coordinates.add(postLongitude);
        venueObj.add("coordinates", coordinates);
        venueObj.addProperty("foursquare", venue.getId());
        System.out.println(venueObj.toString());
        for (int i = 0; i < hereNowCount; i++) {
            dbClient.save(venueObj);
        }
    }


}
