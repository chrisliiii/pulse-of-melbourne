package Server;

import Consumers.*;
import com.google.gson.JsonObject;
import org.apache.log4j.PropertyConfigurator;
import org.lightcouch.CouchDbClient;
import org.lightcouch.CouchDbProperties;

import java.util.List;

/**
 * Created by geoffcunliffe on 20/11/2016.
 */
public class StreamServer {

        public static void main(String[] args){

            String log4jConfPath = "log4j.properties";
            PropertyConfigurator.configure(log4jConfPath);

            // CouchDB Properties
            CouchDbProperties properties = new CouchDbProperties()
                    .setDbName("locations")
                    .setCreateDbIfNotExist(true)
                    .setProtocol("http")
                    .setHost("115.146.95.111")
                    .setPort(5984)
//                    .setUsername("admin")
//                    .setPassword("admin")
                    .setMaxConnections(100)
                    .setConnectionTimeout(0);

            // Twitter authentication keys and secrets
            CouchDbClient twitterDbClient = new CouchDbClient(properties);
            String twitterConsumerKey = "S1zmBVq1A8g3OpoClrCTywFUx";
            String twitterConsumerSecret = "q0DR1ArU0Q8lbNbQywH3mATk0uZU9XVEF6egxCAuHE2Ak682U8";
            String twitterAccessTokenKey = "99900019-Mhdk686sD0KWJdHOLs56bRYxY1jItc7NPt0uqn1aX";
            String twitterAccessTokenSecret = "cIfcXX6E0XDVIabkyWERCHrnnz4yULOwmDBhue236kxxV";

            // Flickr authentication keys and secrets
            CouchDbClient flickrDbClient = new CouchDbClient(properties);
            String flickrApiKey = "cf01dcdbaaa0f0afd57c23a5544280c5";
            String flickrSecret = "bc4b27a72a83be6b";

            // Instagram authentication keys and secrets
//            CouchDbClient instagramDbClient = new CouchDbClient(properties);
            String instagramClientID = "73c1438ae53348b685750beff618915c";
            String instagramClientSecret = "46e8510efdb94050b9528ce92be5e77f";
            String instagramCallbackUrl = "http://geoffcunliffe.com";
//            String instagramToken = "1447623382.73c1438.9167f0ee6f304334a022407f6441d9a5";
            // get tokem from http://services.chrisriversdesign.com/instagram-token/
            String instagramToken = "1447623382.e029fea.74bded2cf2a549899d9838a4b81ad63f";

            //Foursquare authentication keys and secrets
//            CouchDbClient foursquareDbClient = new CouchDbClient(properties);
            String foursquareClientID = "EZAFMXHSUXPTKCY1NW5SUMJCHSZZ3YMJ5ATFL4UEVOX2DZYO";
            String foursquareClientSecret = "ANATUB424W0ULTMV5CSJK3PNHCFIRQBKLA54EPDOZIFUF100";
            String foursquareCallbackUrl = "http://geoffcunliffe.com";

            // //Foursquare authentication keys and secrets
//            CouchDbClient youtubeDbClient = new CouchDbClient(properties);
            String googleApiKey = "AIzaSyDc9NTiQlZY10g-cPxlUJ1hMRUgPCagfoE";


            final TwitterConsumer twitterConsumer = new TwitterConsumer(twitterDbClient,twitterConsumerKey,twitterConsumerSecret,twitterAccessTokenKey,twitterAccessTokenSecret);
            final FlickrConsumer flickrConsumer = new FlickrConsumer(flickrDbClient,flickrApiKey,flickrSecret);
//            final InstagramConsumer instagramConsumer = new InstagramConsumer(instagramDbClient,instagramClientID,instagramClientSecret,instagramCallbackUrl,instagramToken);
//            final FoursquareConsumer foursquareConsumer = new FoursquareConsumer(foursquareDbClient,foursquareClientID,foursquareClientSecret,foursquareCallbackUrl);
//            final YoutubeConsumer youtubeConsumer = new YoutubeConsumer(youtubeDbClient,googleApiKey);

            twitterConsumer.start();
//            flickrConsumer.start();
//            instagramConsumer.start();
//            foursquareConsumer.start();
//            youtubeConsumer.start();



//            Object allDocs = flickrDbClient.find(Object.class, "_design/_latest_flickr");
//            Object allDocs = flickrDbClient.view("test/_view/test").descending(true).limit(1).query(Object.class);

        }
    }
