package Server;

import Consumers.*;
import com.twitter.hbc.core.endpoint.Location.*;
import org.apache.log4j.PropertyConfigurator;
import org.lightcouch.CouchDbClient;
import org.lightcouch.CouchDbProperties;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class StreamServer {

    private static String city;
    private static String dbHost;
    private static int dbPort;

    public static void main(String[] args) {

        parseArgs(args);

        PropertyConfigurator.configure("log4j.properties");

        System.out.println(dbHost + " worked");

        // CouchDB Properties
        CouchDbProperties properties = new CouchDbProperties()
                .setDbName(city)
                .setCreateDbIfNotExist(true)
                .setProtocol("http")
                .setHost(dbHost)
                .setPort(dbPort)
//                    .setUsername("admin")
//                    .setPassword("admin")
                .setMaxConnections(100)
                .setConnectionTimeout(0);

        String centerLongitude, centerLatitude;
        if (city.equals("melbourne")) {
            centerLatitude = "-37.8136";
            centerLongitude = "144.9631";
        } else {
            centerLatitude = "-33.8688";
            centerLongitude = "151.2093";
        }


        // Twitter authentication keys and secrets
        CouchDbClient twitterDbClient = new CouchDbClient(properties);
        String twitterConsumerKey, twitterConsumerSecret, twitterAccessTokenKey, twitterAccessTokenSecret;
        Coordinate twitterSouthwest, twitterNortheast;
        if (city.equals("melbourne")) {             //Melbourne
            twitterConsumerKey = "S1zmBVq1A8g3OpoClrCTywFUx";
            twitterConsumerSecret = "q0DR1ArU0Q8lbNbQywH3mATk0uZU9XVEF6egxCAuHE2Ak682U8";
            twitterAccessTokenKey = "99900019-Mhdk686sD0KWJdHOLs56bRYxY1jItc7NPt0uqn1aX";
            twitterAccessTokenSecret = "cIfcXX6E0XDVIabkyWERCHrnnz4yULOwmDBhue236kxxV";
            twitterSouthwest = new Coordinate(144.9061, -37.8549);
            twitterNortheast = new Coordinate(145.0200, -37.7686);
        } else {            //Sydney
            twitterConsumerKey = "wlBxrOHMOXItd5Oaquqbo2l8m";
            twitterConsumerSecret = "hA9ZRhGG5Pu9DObBLSQdjhr0XZALNuDjQPaxYWrHKmgsFmmZV4";
            twitterAccessTokenKey = "810701712147288064-83etJMinfKehz5aEa80fsJ7pl8AUQbt";
            twitterAccessTokenSecret = "2rWPJSZJZgfD3JKXdunwr7N3qFn2kg6ngauJbhoKJblwi";
            twitterSouthwest = new Coordinate(151.1551, -33.9064);
            twitterNortheast = new Coordinate(151.2689, -33.8201);
        }

        // Flickr authentication keys and secrets
        CouchDbClient flickrDbClient = new CouchDbClient(properties);
        String flickrApiKey = "cf01dcdbaaa0f0afd57c23a5544280c5";
        String flickrSecret = "bc4b27a72a83be6b";

        // Instagram authentication keys and secrets
        CouchDbClient instagramDbClient = new CouchDbClient(properties);

        // Foursquare authentication keys and secrets
        CouchDbClient foursquareDbClient = new CouchDbClient(properties);
        String foursquareClientID = "EZAFMXHSUXPTKCY1NW5SUMJCHSZZ3YMJ5ATFL4UEVOX2DZYO";
        String foursquareClientSecret = "ANATUB424W0ULTMV5CSJK3PNHCFIRQBKLA54EPDOZIFUF100";
        String foursquareCallbackUrl = "http://geoffcunliffe.com";
        double foursquareLatitude;
        double foursquareLongitude;
        if (city.equals("melbourne")) {             //Melbourne
            foursquareLatitude = -33.820142;
            foursquareLongitude = 151.155146;
        } else {
            foursquareLatitude = -37.768648;
            foursquareLongitude = 144.906196;
        }

        // Youtube authentication keys and secrets
        CouchDbClient youtubeDbClient = new CouchDbClient(properties);
        String googleApiKey = "AIzaSyCFvFvg14q1FR4yojBsm4HqeQvDP0zttvI";

        final TwitterConsumer twitterConsumer = new TwitterConsumer(twitterDbClient, twitterConsumerKey, twitterConsumerSecret, twitterAccessTokenKey, twitterAccessTokenSecret, twitterSouthwest, twitterNortheast);
        final FlickrConsumer flickrConsumer = new FlickrConsumer(flickrDbClient, flickrApiKey, flickrSecret, centerLatitude, centerLongitude);
        final InstagramConsumer instagramConsumer = new InstagramConsumer(instagramDbClient, centerLatitude, centerLongitude);
        final FoursquareConsumer foursquareConsumer = new FoursquareConsumer(foursquareDbClient, foursquareClientID, foursquareClientSecret, foursquareCallbackUrl, foursquareLatitude, foursquareLongitude);
        final YoutubeConsumer youtubeConsumer = new YoutubeConsumer(youtubeDbClient, googleApiKey, centerLatitude + "," + centerLongitude);

        twitterConsumer.start();
        instagramConsumer.start();
        ScheduledExecutorService ses = Executors.newScheduledThreadPool(3);
        ses.scheduleAtFixedRate(foursquareConsumer, 0, 1, TimeUnit.HOURS);
        ses.scheduleAtFixedRate(flickrConsumer, 0, 1, TimeUnit.HOURS);
        ses.scheduleAtFixedRate(youtubeConsumer, 0, 1, TimeUnit.HOURS);

    }

    private static void parseArgs(String[] args) {

        String error = "Please use either \"melbourne\" or \"sydney\" and a database \"IP:Port\" as arguments";

        try {
            if (args[0].equals("melbourne") || args[0].equals("sydney") || args[0].equals("locations")) {
                city = args[0];
                dbHost = args[1].split(":")[0];
                dbPort = Integer.parseInt(args[1].split(":")[1]);
            }
            else {
                System.out.println(error);
                System.exit(0);
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            System.out.println(error);
            e.printStackTrace();
            System.exit(0);
        }
    }
}
