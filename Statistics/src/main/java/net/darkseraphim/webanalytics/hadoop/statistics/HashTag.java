package net.darkseraphim.webanalytics.hadoop.statistics;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * @author DarkSeraphim.
 */
public class HashTag implements Writable {

    private static class Day {
        private static final ThreadLocal<Calendar> calendar = new ThreadLocal<Calendar>() {
            @Override
            protected Calendar initialValue() {
                return Calendar.getInstance();
            }
        };

        private final int year;

        private final int day;

        private Day(int year, int day) {
            this.year = year;
            this.day = day;
        }

        private Day(Date date) {
            Calendar c = calendar.get();
            c.setTime(date);
            this.year = c.get(Calendar.YEAR);
            this.day = c.get(Calendar.DAY_OF_YEAR);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Day day1 = (Day) o;
            return year == day1.year && day == day1.day;

        }

        @Override
        public int hashCode() {
            int result = year;
            result = 31 * result + day;
            return result;
        }
    }

    private Map<Day, Integer> countPerDay;

    public HashTag() {
        this.countPerDay = new HashMap<Day, Integer>();
    }

    public HashTag(Date day) {
        this.countPerDay = new HashMap<Day, Integer>();
        this.countPerDay.put(new Day(day), 1);
    }

    void setDate(Date date) {
        this.countPerDay.clear();
        this.countPerDay.put(new Day(date), 1);
    }

    void merge(HashTag tag) {
        this.countPerDay.putAll(tag.countPerDay);
    }

    public void write(DataOutput dataOutput) throws IOException {
        Set<Map.Entry<Day, Integer>> entries = this.countPerDay.entrySet();
        dataOutput.writeInt(entries.size());
        for (Map.Entry<Day, Integer> entry : entries) {
            Day day = entry.getKey();
            dataOutput.writeInt(day.year);
            dataOutput.writeInt(day.day);
            dataOutput.writeInt(entry.getValue());
        }
    }

    public void readFields(DataInput dataInput) throws IOException {
        int counts = dataInput.readInt();
        for (int i = 0; i < counts; i++) {
            int year = dataInput.readInt();
            int day = dataInput.readInt();
            this.countPerDay.put(new Day(year, day), dataInput.readInt());
        }
    }
}
