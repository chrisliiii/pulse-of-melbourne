package Consumers;

import fi.foyt.foursquare.api.FoursquareApi;
import fi.foyt.foursquare.api.FoursquareApiException;
import fi.foyt.foursquare.api.Result;
import fi.foyt.foursquare.api.entities.CompactVenue;
import fi.foyt.foursquare.api.entities.VenuesSearchResult;
import org.lightcouch.CouchDbClient;
import org.omg.CORBA.DoubleHolder;

import static java.lang.Math.cos;


/**
 * Created by geoffcunliffe on 30/11/2016.
 */
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

        //Position, decimal degrees
        double lat = -37.8136;
        double lon = 144.963;

        //Earth’s radius, sphere
        long radius = 6378137;

        //offsets in meters
        int dn = 100;
        int de = 100;

        for (int i = 0; i < 5; i++){

            //Coordinate offsets in radians
            double dLat = dn / radius;
            double dLon = de / (radius * cos(Math.PI * lat / 180));

            //OffsetPosition, decimal degrees
            lat = lat + dLat * 180 / Math.PI;
            lon = lon + dLon * 180 / Math.PI;

            String ll = Double.toString(lat) + "," + Double.toString(lon);
            System.out.println(ll);

            Result<VenuesSearchResult> result = null;
//            Result<CompactVenue[]> result = null;
            try {
//                result = foursquareApi.venuesTrending(ll,50,2000);
                result = foursquareApi.venuesSearch(ll, null, null, null, null, 50, null, null, null, null, null, 100, null);
            } catch (FoursquareApiException e) {
                e.printStackTrace();
            }

            //Result<VenuesSearchResult> result = foursquareApi.venuesSearch();


            if (result.getMeta().getCode() == 200) {
                // if query was ok we can finally we do something with the data
                for (CompactVenue venue : result.getResult().getVenues()) {
//                for (CompactVenue venue : result.getResult()) {
                    System.out.println(venue.getName() + " " + venue.getLocation().getLat() + "," + venue.getLocation().getLng() + " count: " + venue.getHereNow().getCount());
                }
            } else {
                System.out.println("Error occured: ");
                System.out.println("  code: " + result.getMeta().getCode());
                System.out.println("  type: " + result.getMeta().getErrorType());
                System.out.println("  detail: " + result.getMeta().getErrorDetail());
            }

            System.out.println("\n");
        }

    }


    }
