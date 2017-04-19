package Geo;

import com.vividsolutions.jts.geom.*;
import org.geotools.geometry.jts.JTSFactoryFinder;

/**
 * Class BBox will check if a point is in a bounding box, as specified by the city
 */
public class BBox {

    private Polygon polygon;
    GeometryFactory geometryFactory;

    /**
     *  Constructs a BBox object and creates and appropriate polygon
     *  @param  city the name of the city from which to query posts
     */
    public BBox(String city) {

        Coordinate[] coords;

        if (city.equals("melbourne")) {
            coords = new Coordinate[]{new Coordinate(144.9061, -37.8549), new Coordinate(144.9061, -37.7686),
                    new Coordinate(145.0200, -37.7686), new Coordinate(145.0200, -37.8549), new Coordinate(144.9061, -37.8549)};
        } else {
            coords = new Coordinate[]{new Coordinate(151.1551, -33.9064), new Coordinate(151.1551, -33.8201),
                    new Coordinate(151.2689, -33.8201), new Coordinate(151.2689, -33.9064), new Coordinate(151.1551, -33.9064)};
        }

        geometryFactory = JTSFactoryFinder.getGeometryFactory();
        LinearRing ring = geometryFactory.createLinearRing(coords);
        LinearRing holes[] = null; // use LinearRing[] to represent holes
        polygon = geometryFactory.createPolygon(ring, holes);

    }

    /**
     *  Checks whether a point lies in the BBox object's polygon
     */
    public boolean checkPoint(double lon, double lat) {

        Coordinate coord = new Coordinate(lon, lat);
        Point point = geometryFactory.createPoint(coord);

        return polygon.contains(point);

    }


}
