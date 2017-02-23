package FieldCreators;

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;

import java.util.Calendar;
import java.util.Date;

/**
 * Created by geoffcunliffe on 16/02/2017.
 */
public class DateArrayCreator {
    private JsonPrimitive year;
    private JsonPrimitive month;
    private JsonPrimitive day;
    private JsonPrimitive dayOfWeek;
    private JsonPrimitive hour;
    private JsonPrimitive minutes;
    private JsonPrimitive seconds;
    private JsonPrimitive zone;

    public DateArrayCreator(Long timestamp) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(timestamp));

        this.year = new JsonPrimitive(cal.get(Calendar.YEAR));
        this.month = new JsonPrimitive(cal.get(Calendar.MONTH)+1);
        this.day = new JsonPrimitive(cal.get(Calendar.DAY_OF_MONTH));
        this.dayOfWeek = new JsonPrimitive(cal.get(Calendar.DAY_OF_WEEK));
        this.hour = new JsonPrimitive(cal.get(Calendar.HOUR_OF_DAY));
        this.minutes = new JsonPrimitive(cal.get(Calendar.MINUTE));
        this.seconds = new JsonPrimitive(cal.get(Calendar.SECOND));
        this.zone = new JsonPrimitive("AEDT");
    }

    public JsonArray getDateArray() {
        JsonArray dateArray = new JsonArray();

        dateArray.add(year);
        dateArray.add(month);
        dateArray.add(day);
        dateArray.add(hour);
        dateArray.add(minutes);
        dateArray.add(seconds);
        dateArray.add(dayOfWeek);
        dateArray.add(zone);

        return dateArray;
    }
}
