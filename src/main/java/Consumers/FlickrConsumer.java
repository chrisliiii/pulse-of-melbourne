package Consumers;

import com.flickr4java.flickr.Flickr;
import com.flickr4java.flickr.FlickrException;
import com.flickr4java.flickr.REST;
import com.flickr4java.flickr.photos.Photo;
import com.flickr4java.flickr.photos.PhotoList;
import com.flickr4java.flickr.photos.SearchParameters;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.lightcouch.CouchDbClient;
import org.lightcouch.NoDocumentException;

import java.sql.Timestamp;
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

        while (true) {

            try {

                Flickr flickr = new Flickr(apikey, secret, new REST());

                Timestamp dblatest = getLatestDateTaken();

                SearchParameters searchParameters = new SearchParameters();
                searchParameters.setAccuracy(6); //6 is region accuracy
                searchParameters.setHasGeo(true);
                searchParameters.setSort(2); //sorts by date taken descending
                searchParameters.setMinTakenDate(dblatest);
                searchParameters.setBBox("144.3537",
                        "-38.2601",
                        "145.3045",
                        "-37.3040");

                while (true) {


                    //search arguments are parameters,results per query,page number
                    PhotoList<Photo> photoList = flickr.getPhotosInterface().search(searchParameters, 30, 0);
                    Iterator itr = photoList.iterator();

                    if (!itr.hasNext()) {
                        System.out.println("There aren't any results for your FLICKR query. Pausing 30 seconds");
//                    System.out.println(searchParameters.getMinTakenDate());
                        TimeUnit.SECONDS.sleep(30);
                    } else {

                        Photo latestPhoto = flickr.getPhotosInterface().getInfo(photoList.get(0).getId(), secret);
                        saveLatestFlickrDateTaken(latestPhoto);
                        searchParameters.setMinTakenDate(latestPhoto.getDateTaken());

                        while (itr.hasNext()) {
                            Photo photo = (Photo) itr.next();
                            Photo photoWithInfo = flickr.getPhotosInterface().getInfo(photo.getId(), secret);

                            saveToDB(photoWithInfo);

                        }
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (FlickrException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }


    private void saveToDB(Photo photoWithInfo) {
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

    private Timestamp getLatestDateTaken() {
        try {
            JsonObject latestDbObj = dbClient.find(JsonObject.class, "latest_flickr");
            return new Timestamp(latestDbObj.get("latest_flickr").getAsLong());
        } catch (NoDocumentException e) {
            System.out.println("No latest_flickr document found, continuing");
            return new Timestamp(0);
        }
    }

    public void saveLatestFlickrDateTaken(Photo latestFlickrPhoto) {
//        System.out.println(latestFlickrPhoto.getDateTaken());
        JsonObject latestTimeObj = new JsonObject();
        try {
            JsonObject latestDbObj = dbClient.find(JsonObject.class, "latest_flickr");
            String latestRev = latestDbObj.get("_rev").getAsString();
            latestTimeObj.addProperty("_id", "latest_flickr");
            latestTimeObj.addProperty("_rev", latestRev);
            latestTimeObj.addProperty("latest_flickr", latestFlickrPhoto.getDateTaken().getTime());
            dbClient.update(latestTimeObj);
        } catch (NoDocumentException e) {
            System.out.println("No latest_flickr document found, creating.");
            latestTimeObj.addProperty("_id", "latest_flickr");
            latestTimeObj.addProperty("latest_flickr", latestFlickrPhoto.getDateTaken().getTime());
            dbClient.save(latestTimeObj);
        }
    }
}