package FieldCreators;

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;

import java.util.Calendar;
import java.util.Date;
import java.util.SimpleTimeZone;

/**
 * Class DateArrayCreator constructs a Json Array containing separated date details for each social media post
 * in the format [YYYY,MM,DD,Hour,Minute,Second,DayOfWeek,Timezone]. Extracted from a timestamp.
 */
public class DateArrayCreator {

    private JsonArray dateArray;

    /**
     * The DateArrayCreator Constructor constructs the date array field to be saved with a db document
     * @param timestamp The timestamp of a social media post
     */
    public DateArrayCreator(Long timestamp) {
        this.dateArray = new JsonArray();
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(timestamp));

        this.dateArray.add(new JsonPrimitive(cal.get(Calendar.YEAR)));
        this.dateArray.add(new JsonPrimitive(cal.get(Calendar.MONTH)+1));
        this.dateArray.add(new JsonPrimitive(cal.get(Calendar.DAY_OF_MONTH)));
        this.dateArray.add(new JsonPrimitive(cal.get(Calendar.HOUR_OF_DAY)));
        this.dateArray.add(new JsonPrimitive(cal.get(Calendar.MINUTE)));
        this.dateArray.add(new JsonPrimitive(cal.get(Calendar.SECOND)));
        this.dateArray.add(new JsonPrimitive(cal.get(Calendar.DAY_OF_WEEK)));
        if(SimpleTimeZone.getTimeZone("Australia/Melbourne").inDaylightTime(new Date(timestamp)))
            this.dateArray.add(new JsonPrimitive("AEDT"));
        else this.dateArray.add(new JsonPrimitive("AEST"));
    }

    public JsonArray getDateArray() {
        return this.dateArray;
    }
}
