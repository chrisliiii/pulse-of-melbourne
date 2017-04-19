package StreamConsumers;

import DBDocumentHandlers.LatestDocumentHandler;
import FieldCreators.CoordinatesCreator;
import FieldCreators.DBEntryConstructor;
import FieldCreators.DateArrayCreator;
import Geo.SuburbFinder;
import KeyHandlers.FlickrKeyHandler;
import com.flickr4java.flickr.Flickr;
import com.flickr4java.flickr.FlickrException;
import com.flickr4java.flickr.REST;
import com.flickr4java.flickr.photos.Photo;
import com.flickr4java.flickr.photos.PhotoList;
import com.flickr4java.flickr.photos.SearchParameters;
import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;
import org.lightcouch.CouchDbClient;
import org.lightcouch.CouchDbProperties;

import java.util.Iterator;

/**
 * Class FlickrConsumer uses the Flickr4Java API to access and retrieve the most
 * recent flickr posts. A query is made to the StreamServer, getting the 20 latest posts
 * within a city with regional accuracy. After comparison to the most recent document
 * in the database, the newer posts are saved.<p>
 * API : https://github.com/boncey/Flickr4Java
 */

public class FlickrConsumer extends Thread {

    private final String latitude, longitude;
    private CouchDbClient dbClient;
    private FlickrKeyHandler flickrKeyHandler;
    private Flickr flickr;
    private String city;

    /**
     *  Constructs a consumer object, setting the location, database client, and retrieving the API key(s)
     * @param  KEYFILE the filename of the file containing all API keys
     * @param  city the name of the city from which to query posts
     * @param  properties CouchDB client properties
     */
    public FlickrConsumer(String KEYFILE, String city, CouchDbProperties properties) {
        if (city.equals("melbourne")) {
            this.latitude = "-37.8136";
            this.longitude = "144.9631";
        } else {
            this.latitude = "-33.8688";
            this.longitude = "151.2093";
        }
        this.city = city;
        this.flickrKeyHandler = new FlickrKeyHandler(KEYFILE);
        this.dbClient = new CouchDbClient(properties);
    }

    /**
     *  Starts the consumer thread to retrieve latest posts
     */
    public void run() {

        try {

            //Create a new flickr object using the keys
            flickr = new Flickr(flickrKeyHandler.getFlickrApiKey(), flickrKeyHandler.getFlickrSecret(), new REST());

            //This gets the most recent flickr retrieval timestamp from Couchdb if it exists.
            //It is used to save only new posts
            String latestID = "latest_flickr";
            LatestDocumentHandler handler = new LatestDocumentHandler(dbClient);
            long dbLatest = handler.getLatestDate(latestID).getValue();

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
            Photo latestPhoto = flickr.getPhotosInterface().getInfo(photoList.get(0).getId(), flickrKeyHandler.getFlickrSecret());
            handler.saveLatestDate(latestID,latestPhoto.getDatePosted().getTime());

            //Saves the retrieved photos that are newer than the most recent retreival date
            while (itr.hasNext()) {
                Photo photo = (Photo) itr.next();

                //queries the flickr server again to retreive an individual photo's details
                Photo photoWithInfo = flickr.getPhotosInterface().getInfo(photo.getId(), flickrKeyHandler.getFlickrSecret());

                //Compares the date to see if it's a newer object
                if (photoWithInfo.getDatePosted().getTime() > dbLatest) {
                    DBEntryConstructor dbEntry = getFields(photoWithInfo);
//                    System.out.println(dbEntry.getdbObject().toString());
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
        SuburbFinder suburbFinder = new SuburbFinder(latitude,longitude, city);
        String suburb = suburbFinder.getSuburb();

        String user = photoWithInfo.getOwner().getId();
        String text;
        if (!photoWithInfo.getTitle().isEmpty()) {
            text = photoWithInfo.getTitle();
        } else text = null;

        return new DBEntryConstructor(timestamp,dateArray,coordinates,"flickr",user,text,suburb);

    }

}