package net.darkseraphim.webanalytics.hadoop.feels;

import net.darkseraphim.webanalytics.hadoop.csv.CSVTextInputFormat;
import net.darkseraphim.webanalytics.hadoop.csv.Row;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Sentiment analysis skeleton TODO: replace FeelingTweet with desired output
 * class
 */
public class Main {

    private static final String SECOND_OUTPUT = "top10";
    static FeelingTweet feelingTweet;
    static ArrayList<FeelingTweet> foundFeelings = new ArrayList<FeelingTweet>();

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Row, Text, FeelingTweet> {

        private static final SimpleDateFormat sdf = new SimpleDateFormat("");
        private Text tag = new Text();

        public void map(LongWritable key, Row value, OutputCollector<Text, FeelingTweet> output, Reporter reporter) throws IOException {
            // TODO: convert Rows into FeelingTweet instances, call output.collect(key, value) to produce results

            String line = ""; // tweet context
            String feeling = "";
            String location = "";
            String season = "";
            String dateDay = "";
            if (dateDay.contains("-06-") || dateDay.contains("-07-") || dateDay.contains("-08-")) { //Summer tweets
                dateDay = (value.get(5).toString());
                line = value.get(4).toString();
                StringTokenizer stringTokenizer = new StringTokenizer(line);
                while (stringTokenizer.hasMoreTokens()) {
                    String token = stringTokenizer.nextToken();
                    if (token.startsWith("feel")) {
                        if (line.contains("I feel") || line.contains("I would feel")) {
                            feeling = stringTokenizer.nextToken();
                            feelingTweet = new FeelingTweet(feeling, line, location, dateDay, "summer");
                            foundFeelings.add(feelingTweet);
                            //TODO: output
                        }
                    }
                }
            }
            if (dateDay.contains("-09-") || dateDay.contains("-10-") || dateDay.contains("-11-")) { //Summer tweets
                dateDay = (value.get(5).toString());
                line = value.get(4).toString();
                StringTokenizer stringTokenizer = new StringTokenizer(line);
                while (stringTokenizer.hasMoreTokens()) {
                    String token = stringTokenizer.nextToken();
                    if (token.startsWith("feel")) {
                        if (line.contains("I feel") || line.contains("I would feel")) {
                            feeling = stringTokenizer.nextToken();
                            feelingTweet = new FeelingTweet(feeling, line, location, dateDay, "autumn");
                            foundFeelings.add(feelingTweet);
                            //TODO: output
                        }
                    }
                }
            }
            if (dateDay.contains("-12-") || dateDay.contains("-01-") || dateDay.contains("-02-")) { //Summer tweets
                dateDay = (value.get(5).toString());
                line = value.get(4).toString();
                StringTokenizer stringTokenizer = new StringTokenizer(line);
                while (stringTokenizer.hasMoreTokens()) {
                    String token = stringTokenizer.nextToken();
                    if (token.startsWith("feel")) {
                        if (line.contains("I feel") || line.contains("I would feel")) {
                            feeling = stringTokenizer.nextToken();
                            feelingTweet = new FeelingTweet(feeling, line, location, dateDay, "winter");
                            foundFeelings.add(feelingTweet);
                            //TODO: output
                        }
                    }
                }
            }
            if (dateDay.contains("-03-") || dateDay.contains("-04-") || dateDay.contains("-05-")) { //Summer tweets
                dateDay = (value.get(5).toString());
                line = value.get(4).toString();
                StringTokenizer stringTokenizer = new StringTokenizer(line);
                while (stringTokenizer.hasMoreTokens()) {
                    String token = stringTokenizer.nextToken();
                    if (token.startsWith("feel")) {
                        if (line.contains("I feel") || line.contains("I would feel")) {
                            feeling = stringTokenizer.nextToken();
                            feelingTweet = new FeelingTweet(feeling, line, location, dateDay, "spring");
                            foundFeelings.add(feelingTweet);
                            //TODO: output
                        }
                    }
                }
            }

        }
    }

    // TODO: if we don't need to produce multiple outputs, clean up the Reducer            // TODO: if we don't need to produce multiple outputs, clean up the Reducer

    public static class Reduce extends MapReduceBase implements Reducer<Text, FeelingTweet, Text, FeelingTweet> {

        private int ordinal = 0;

        private MultipleOutputs out;

        @Override
        public void configure(JobConf job) {
            this.out = new MultipleOutputs(job);
        }

        public void reduce(Text key, Iterator<FeelingTweet> values, OutputCollector<Text, FeelingTweet> output, Reporter reporter) throws IOException {
            // TODO: reduce all values in the Iterator to one single FeelingTweet instance
        }

        @SuppressWarnings("unchecked")
        private <K, V> OutputCollector<K, V> getOutputCollector(Reporter reporter) throws IOException {
            return ((OutputCollector<K, V>) this.out.getCollector(SECOND_OUTPUT, reporter));
        }

        private void analyse(FeelingTweet tag, OutputCollector<FeelingTweet, Object> output) {
            // TODO: produce second output (if required)
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(Main.class);
        conf.setJobName("feels-analysis");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(FeelingTweet.class);
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setNumReduceTasks(1);
        conf.setInputFormat(CSVTextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        // TODO: determine whether we need extra output
        MultipleOutputs.addMultiNamedOutput(conf, SECOND_OUTPUT, TextOutputFormat.class, Text.class, FeelingTweet.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }

}
