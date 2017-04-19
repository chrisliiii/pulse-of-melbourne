package FieldCreators;

import com.google.gson.JsonPrimitive;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.opengis.filter.Filter;

import java.io.File;
import java.io.IOException;

/**
 * Created by geoffcunliffe on 7/03/2017.
 */
public class SuburbFinder {

    private String suburb;

    public SuburbFinder(JsonPrimitive latitude, JsonPrimitive longitude, String city) {

        File file = new File("shapefiles/" + city + ".shp");

        try{

            Filter filter = CQL.toFilter("CONTAINS(the_geom, POINT(" + longitude.getAsString() + " " + latitude.getAsString() + "))");
            FileDataStore store = FileDataStoreFinder.getDataStore(file);
            SimpleFeatureSource featureSource = store.getFeatureSource();
            SimpleFeatureCollection collection = featureSource.getFeatures(filter);
            SimpleFeatureIterator iterator = collection.features();

            if (iterator.hasNext()) {
                this.suburb = iterator.next().getAttribute(7).toString().toLowerCase();
            } else this.suburb = "unknown";
            iterator.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (CQLException e) {
            e.printStackTrace();
        }

    }

    public String getSuburb() {
        return suburb;
    }
}
