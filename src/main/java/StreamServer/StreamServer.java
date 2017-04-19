/**
 * Created by Geoff Cunliffe. Last update on 24/02/2017.
 * <p>
 * The Pulse of Melbourne Project harvests geospatial data from multiple social media platforms (Twitter, Flickr,
 * FourSquare, Instagram, and Youtube) for the purpose of analysing location hotspots over time.  The motivation
 * behind the project is to allow analysis of behavioural patterns in major cities, namely Meblourne and Sydney
 * with possible applications being road and urban planning, identifying cultural communities, event marketing,
 * semantic analysis, etc.
 * <p>
 * The social media queries are done by creating "consumer" objects for each social media platform, each of which
 * extends thread. The consumer threads are then started, either perpetually or recurring at a set interval,
 * depending on the retrieval method. Each social media platform is connected to using an existing Java API,
 * specified by a Maven repository and managed in the pom.xml file. Please see each consumer class for
 * details on Github sources.
 */
package StreamServer;

import StreamConsumers.*;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.simple.SimpleFeatureSource;
import org.lightcouch.CouchDbProperties;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

 /**
  * Class StreamServer contains the main method of the project, creating all social media consumer objects and starting
  * threads as appropriate.
  */
public class StreamServer {

    private static final String KEYFILE = "consumer.keys";
    private static final String LOG4JPROPERTIES = "log4j.properties";
    private static String city;
    private static String dbHost;
    private static int dbPort;

     /**
      * The main method takes as command line arguments a city name (either melbourne or sydney) and a CouchDB database
      * location in the form of IP:Port, for example 192.168.1.10:5984. After connectivity is confirmed, all
      * necessary consumer threads are started.
      */
    public static void main(String[] args) {

        // Load log4j properties from file
        PropertyConfigurator.configure(LOG4JPROPERTIES);

        //Parse command line arguments
        parseArgs(args);

        //Test database connectivity
        testDBConnection();

        // CouchDB Properties
        CouchDbProperties properties = new CouchDbProperties()
                .setDbName(city)
                .setProtocol("http")
                .setHost(dbHost)
                .setPort(dbPort)
                .setMaxConnections(100)
                .setConnectionTimeout(0);

        // Create new social media consumer objects
        final TwitterConsumer twitterConsumer = new TwitterConsumer(KEYFILE, city, properties);
        final TwitterQuerier twitterQuerier = new TwitterQuerier(KEYFILE, city, properties);
        final FlickrConsumer flickrConsumer = new FlickrConsumer(KEYFILE, city, properties);
        final InstagramConsumer instagramConsumer = new InstagramConsumer(KEYFILE, city, properties);
        final FoursquareConsumer foursquareConsumer = new FoursquareConsumer(KEYFILE, city, properties);
        final YoutubeConsumer youtubeConsumer = new YoutubeConsumer(KEYFILE, city, properties);

        // Start the threads of each consumer.
        // twitterConsumer relies on a stream so only requires to be started once.
        // instagramConsumer has an internal while loop that re-queries the server every few seconds
        // foursquareConsumer, flickrConsumer, and youtubeConsumer are all started at an hourly intervals
        //       due to a lower update rate.
        twitterConsumer.start();
        twitterQuerier.start();
        instagramConsumer.start();
        ScheduledExecutorService ses = Executors.newScheduledThreadPool(3);
        ses.scheduleAtFixedRate(foursquareConsumer, 0, 1, TimeUnit.HOURS);
        ses.scheduleAtFixedRate(flickrConsumer, 0, 1, TimeUnit.HOURS);
        ses.scheduleAtFixedRate(youtubeConsumer, 0, 1, TimeUnit.HOURS);
    }


     // Method parseArgs parses the command line arguments to set the query location city and the database
     // connectivity information
    private static void parseArgs(String[] args) {

        String error = "Error parsing arguments.\n" +
                "Please use either \"melbourne\" or \"sydney\" and a database \"IP:Port\" as arguments \n" +
                "For example java -jar pulse.jar melbourne 192.168.1.10:5984";

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
            System.exit(0);
        }
    }

     //Method testDBConnection tests connection to the CouchDB database and quits if unsuccessful
     private static void testDBConnection() {

         String URL = "http://" + dbHost + ":" + dbPort;

         try {
             java.net.URL url = new URL(URL);
             HttpURLConnection request = (HttpURLConnection) url.openConnection();
             request.connect();
             JsonParser jsonParser = new JsonParser();
             //Convert the input stream to a json element
             JsonObject result = jsonParser.parse(new InputStreamReader((InputStream) request.getContent())).getAsJsonObject();
             if (result.get("couchdb").getAsString().equals("Welcome"))
                 System.out.println("Successfully connected to " + dbHost + " on port " + dbPort);
         } catch (ConnectException e) {
             System.out.println(e.getMessage());
             System.out.println("Connection to " + dbHost + " on port " + dbPort + " was unsuccessful \n" +
                     "Please check database details and try again");
             System.exit(0);
         } catch (MalformedURLException e) {
             e.printStackTrace();
         } catch (IOException e) {
             e.printStackTrace();
         }
     }
}
