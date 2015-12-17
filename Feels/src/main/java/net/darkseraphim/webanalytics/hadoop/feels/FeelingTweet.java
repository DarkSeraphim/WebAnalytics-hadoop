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
    
    
    public FeelingTweet(String feeling, String tweet, String location, String date, String season) {
        
    }
    public void write(DataOutput dataOutput) throws IOException {
        // TODO: write fields to DataOutput
    }

    public void readFields(DataInput dataInput) throws IOException {
        // TODO: read fields from DataOutput
    }
}
