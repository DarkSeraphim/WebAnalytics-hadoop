package net.darkseraphim.webanalytics.hadoop.feels;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author DarkSeraphim.
 */
public class FeelingTweet implements Writable {

    /*
     *  The following two methods are for persistence
     *  Any field which should be saved whenever we transfer data between Hadoop instances, should be written
     *  to the DataOutput, and read from the DataInput (in the same order, when we call read & write, the state
     *  of the object should not change!)
     */
    public String feeling = "";
    public String tweet = "";
    public String loc = "";
    public String date = "";
    public String season = "";
    
    
    public FeelingTweet(String feeling_O, String tweet_O, String location_O, String date_O, String season_O) {
        feeling = feeling_O;
        tweet = tweet_O;
        loc = location_O;
        date = date_O;
        season = season_O;    
       
    }
    
    String getFeeling() {
        
        return feeling;
    }
    
    String getTweet() {
        return tweet;
    }
    
    String getLocation() {
        return loc;
    }
    
    String getDate() {
        return date;
    }
    
    String getSeason() {
        return season;
    }
    public void write(DataOutput dataOutput) throws IOException {
        // TODO: write fields to DataOutput
    }

    public void readFields(DataInput dataInput) throws IOException {
        // TODO: read fields from DataOutput
    }
}
