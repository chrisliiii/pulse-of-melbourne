package FieldCreators;

import com.google.api.client.util.DateTime;
import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;

import java.util.Calendar;
import java.util.Date;
import java.util.SimpleTimeZone;

/**
 * Created by geoffcunliffe on 16/02/2017.
 *
 *
 */
public class DateArrayCreator {

    private JsonArray dateArray;

    public DateArrayCreator(Long timestamp) {
        dateArray = new JsonArray();
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(timestamp));

        dateArray.add(new JsonPrimitive(cal.get(Calendar.YEAR)));
        dateArray.add(new JsonPrimitive(cal.get(Calendar.MONTH)+1));
        dateArray.add(new JsonPrimitive(cal.get(Calendar.DAY_OF_MONTH)));
        dateArray.add(new JsonPrimitive(cal.get(Calendar.HOUR_OF_DAY)));
        dateArray.add(new JsonPrimitive(cal.get(Calendar.MINUTE)));
        dateArray.add(new JsonPrimitive(cal.get(Calendar.SECOND)));
        dateArray.add(new JsonPrimitive(cal.get(Calendar.DAY_OF_WEEK)));
        if(SimpleTimeZone.getTimeZone("Australia/Melbourne").inDaylightTime(new Date(timestamp)))
            dateArray.add(new JsonPrimitive("AEDT"));
        else dateArray.add(new JsonPrimitive("AEST"));
    }

    public JsonArray getDateArray() {
        return this.dateArray;
    }
}
