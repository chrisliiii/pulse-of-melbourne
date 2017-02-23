package Consumers;

import DBConstructor.DBEntryConstructor;
import FieldCreators.CoordinatesCreator;
import FieldCreators.DateArrayCreator;
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

import java.util.Iterator;

/**
 * Created by geoffcunliffe on 17/12/2017.
 *
 * Class FlickrConsumer uses the Flickr4Java API to access and retrieve the most recent flickr posts
 */
public class FlickrConsumer extends Thread {

    private final String apikey, secret, latitude, longitude;
    private CouchDbClient dbClient;

    public FlickrConsumer(CouchDbClient dbClient, String apikey, String secret, String latitude, String longitude) {
        this.dbClient = dbClient;
        this.apikey = apikey;
        this.secret = secret;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public void run() {

        try {

            //Create a new flickr object using the keys
            Flickr flickr = new Flickr(apikey, secret, new REST());

            //This gets the most recent flickr retrieval timestamp from Couchdb if it exists.
            //It is used to save only new posts
            long dblatest = getLatestDateUpdated();

            //Sets the search parameters for the image search, specifying specific coordinates.
            //The default search radius is 5km
            SearchParameters searchParameters = new SearchParameters();
            searchParameters.setAccuracy(6); //6 is region accuracy
            searchParameters.setHasGeo(true);
            searchParameters.setSort(0); //sorts by date posted descending
            searchParameters.setLatitude(latitude);
            searchParameters.setLongitude(longitude);

            //This performs a query to the flickr server and returns a simple list of the most recent photos
            //search arguments are parameters,results per query,page number
            PhotoList<Photo> photoList = flickr.getPhotosInterface().search(searchParameters, 500, 0);

            Iterator itr = photoList.iterator();

            //Gets the most recent photo and updates the latest retrieval date
            //in the CouchDB database for date checking purposes
            Photo latestPhoto = flickr.getPhotosInterface().getInfo(photoList.get(0).getId(), secret);
            saveLatestFlickrDatePosted(latestPhoto);

            //Saves the retrieved photos that are newer than the most recent retreival date
            while (itr.hasNext()) {
                Photo photo = (Photo) itr.next();

                //queries the flickr server again to retreive an individual photo's details
                Photo photoWithInfo = flickr.getPhotosInterface().getInfo(photo.getId(), secret);

                //Compares the date to see if it's a newer object
                if (photoWithInfo.getDatePosted().getTime() > dblatest) {
                    DBEntryConstructor dbEntry = getFields(photoWithInfo);
                    System.out.println(dbEntry.getdbObject().toString());
                    dbClient.save(dbEntry.getdbObject());
                }

                else break;
            }


        } catch (FlickrException e) {
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("There is something wrong with flickr, retrying at next interval");
            e.printStackTrace();
        }
    }

    //Method getFields extracts the required info from the passed in retrieved post
    private DBEntryConstructor getFields(Photo photoWithInfo) {

        long timestamp = photoWithInfo.getDateTaken().getTime();
        DateArrayCreator dateArrayCreator = new DateArrayCreator(photoWithInfo.getDateTaken().getTime());
        JsonArray dateArray = dateArrayCreator.getDateArray();

        JsonPrimitive latitude = new JsonPrimitive(photoWithInfo.getGeoData().getLatitude());
        JsonPrimitive longitude = new JsonPrimitive(photoWithInfo.getGeoData().getLongitude());
        CoordinatesCreator coordinatesCreator = new CoordinatesCreator(latitude,longitude);
        JsonArray coordinates = coordinatesCreator.getCoordinates();

        String user = photoWithInfo.getOwner().getId();
        String text;
        if (!photoWithInfo.getTitle().isEmpty()) {
            text = photoWithInfo.getTitle();
        } else text = null;

        return new DBEntryConstructor(timestamp,dateArray,coordinates,"flickr",user,text);

    }

    //Method getLatestDateUpdated returns the timestamp of the newest flickr post from
    //previous search results, stored in a "latest_flickr" document in CouchDb
    private long getLatestDateUpdated() {
        try {
            JsonObject latestDbObj = dbClient.find(JsonObject.class, "latest_flickr");
            return latestDbObj.get("latest_flickr").getAsLong();
        } catch (NoDocumentException e) {
            System.out.println("No latest_flickr document found, continuing");
            return 0;
        }
    }

    //Method saveLatestFlickrDatePosted updates a "latest_flickr" object in the database
    //to contain the most recent timestamp from the latest retrieval from the flickr server.
    //This is then compared to new search results to only save the newest data
    public void saveLatestFlickrDatePosted(Photo latestFlickrPhoto) {

        JsonObject latestTimeObj = new JsonObject();
        String id = "latest_flickr";
        try {
            JsonObject latestDbObj = dbClient.find(JsonObject.class, id);
            latestDbObj.remove(id);
            latestDbObj.addProperty(id, latestFlickrPhoto.getDatePosted().getTime());
            dbClient.update(latestDbObj);
        } catch (NoDocumentException e) {
            System.out.println("No latest_flickr document found, creating.");
            latestTimeObj.addProperty("_id", id);
            latestTimeObj.addProperty(id, latestFlickrPhoto.getDatePosted().getTime());
            dbClient.save(latestTimeObj);
        }
    }


}