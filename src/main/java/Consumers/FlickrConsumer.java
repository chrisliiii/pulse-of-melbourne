package Consumers;

import com.flickr4java.flickr.Flickr;
import com.flickr4java.flickr.FlickrException;
import com.flickr4java.flickr.REST;
import com.flickr4java.flickr.photos.Photo;
import com.flickr4java.flickr.photos.PhotoList;
import com.flickr4java.flickr.photos.SearchParameters;
import com.google.api.client.util.DateTime;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.codehaus.groovy.runtime.ReverseListIterator;
import org.lightcouch.CouchDbClient;
import org.lightcouch.CouchDbProperties;

import java.security.Timestamp;
import java.sql.Date;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;


public class FlickrConsumer extends Thread {

    private final String apikey;
    private final String secret;
    private CouchDbClient dbClient;

    public FlickrConsumer(CouchDbClient dbClient, String apikey, String secret) {
        this.dbClient = dbClient;
        this.apikey = apikey;
        this.secret = secret;
    }

    public void run() {

        Flickr flickr = new Flickr(apikey, secret, new REST());
        SearchParameters searchParameters = new SearchParameters();
        searchParameters.setAccuracy(6); //6 is region accuracy
        searchParameters.setHasGeo(true);
        searchParameters.setBBox("144.3537",
                "-38.2601",
                "145.3045",
                "-37.3040");

        while(true) {
            try {
                //search arguments are parameters,results per query,page number
                PhotoList<Photo> photoList = flickr.getPhotosInterface().search(searchParameters,0,0);
                Iterator itr = photoList.iterator();
                Photo latest = flickr.getPhotosInterface().getInfo(photoList.get(0).getId(), secret);

                if (!itr.hasNext()) {
                    System.out.println("no more for now");
                    TimeUnit.SECONDS.sleep(30);
                }
                else {
                    Long allDocs = dbClient.view("_latest_flickr/_view").descending(true).limit(1).queryForLong();
//                    searchParameters.setMinUploadDate(new java.sql.Timestamp(latest.getLastUpdate().getTime()));

                    while (itr.hasNext()) {
                        Photo photo = (Photo) itr.next();
                        Photo photoWithInfo = flickr.getPhotosInterface().getInfo(photo.getId(), secret);

                        JsonArray coordinates = new JsonArray();
                        JsonObject flickrObj = new JsonObject();

                        flickrObj.addProperty("timestamp", photoWithInfo.getDateTaken().getTime());
                        JsonPrimitive latitude = new JsonPrimitive(photoWithInfo.getGeoData().getLatitude());
                        JsonPrimitive longitude = new JsonPrimitive(photoWithInfo.getGeoData().getLongitude());
                        coordinates.add(latitude);
                        coordinates.add(longitude);
                        flickrObj.add("coordinates", coordinates);
                        flickrObj.addProperty("flickr", photoWithInfo.getOwner().getId());
                        System.out.println(flickrObj.toString());
                        dbClient.save(flickrObj);

                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (FlickrException e) {
                e.printStackTrace();
            }

        }
    }
}