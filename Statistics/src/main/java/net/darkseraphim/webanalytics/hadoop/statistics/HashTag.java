package net.darkseraphim.webanalytics.hadoop.statistics;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * @author DarkSeraphim.
 */
public class HashTag implements Writable, WritableComparable<HashTag> {

    private static class Statistic implements Writable {
        public void write(DataOutput dataOutput) throws IOException {

        }

        public void readFields(DataInput dataInput) throws IOException {

        }
    }

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

    private int rank = -1;

    private String tag;

    private Statistic statistic;

    private HashTag() {
        this.countPerDay = new HashMap<Day, Integer>();
    }

    public HashTag(String tag) {
        this.tag = tag;
        this.countPerDay = new HashMap<Day, Integer>();
    }

    public HashTag(String tag, Date day) {
        this.tag = tag;
        this.countPerDay = new HashMap<Day, Integer>();
        this.countPerDay.put(new Day(day), 1);
    }

    String getTag() {
        return this.tag;
    }

    void setDate(Date date) {
        this.countPerDay.clear();
        this.countPerDay.put(new Day(date), 1);
    }

    void merge(HashTag tag) {
        for (Map.Entry<Day, Integer> entry : tag.countPerDay.entrySet()) {
            Integer i = this.countPerDay.get(entry.getKey());
            this.countPerDay.put(entry.getKey(), (i != null ? i + entry.getValue() : entry.getValue()));
        }
    }

    int getCount() {
        int sum = 0;
        for (Integer i : this.countPerDay.values()) {
            if (i != null) {
                sum += i;
            }
        }
        return sum;
    }

    void setRank(int rank) {
        this.rank = rank;
    }

    int getRank() {
        return this.rank;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.tag);
        dataOutput.writeInt(this.rank);
        dataOutput.writeBoolean(this.statistic != null);
        if (this.statistic != null) {
            this.statistic.write(dataOutput);
        }
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
        this.tag = dataInput.readUTF();
        this.rank = dataInput.readInt();
        if (dataInput.readBoolean()) {
            this.statistic = new Statistic();
            this.statistic.readFields(dataInput);
        }
        int counts = dataInput.readInt();
        for (int i = 0; i < counts; i++) {
            int year = dataInput.readInt();
            int day = dataInput.readInt();
            this.countPerDay.put(new Day(year, day), dataInput.readInt());
        }
    }

    public int compareTo(HashTag o) {
        int oc = o.getCount();
        int c = this.getCount();
        if (oc != c) {
            return oc > c ? 1 : oc < c ? -1 : 0;
        }
        return this.tag.compareTo(o.getTag());
    }

    @Override
    public String toString() {
        return this.tag;
    }
}
