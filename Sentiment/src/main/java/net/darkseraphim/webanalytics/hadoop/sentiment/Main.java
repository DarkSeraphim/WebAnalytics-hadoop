package net.darkseraphim.webanalytics.hadoop.sentiment;

import net.darkseraphim.webanalytics.hadoop.csv.CSVTextInputFormat;
import net.darkseraphim.webanalytics.hadoop.csv.Row;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Sentiment analysis skeleton
 * TODO: replace TheOutputClass with desired output class
 */
public class Main {

    private static final String SECOND_OUTPUT = "top10";

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Row, Text, TheOutputClass> {
        private static final SimpleDateFormat sdf = new SimpleDateFormat("");
        private Text tag = new Text();

        public void map(LongWritable key, Row value, OutputCollector<Text, TheOutputClass> output, Reporter reporter) throws IOException {
            // TODO: convert Rows into TheOutputClass instances, call output.collect(key, value) to produce results
            String line = ""; // tweet context
            String dateTimeline = ""; //constructing a timeline           
            String dateDay = (""+value.get(5).toString());
            if (dateDay.substring(0, 9).equals("2013-12-31") || dateDay.substring(0, 9).equals("2014-01-01")  ) { //Restrict tweets by date range dec 2013 - jan 2014
                dateTimeline = dateDay.substring(5); //Timeline                
                line = value.get(4).toString();
                StringTokenizer stringTokenizer = new StringTokenizer(line);
                while (stringTokenizer.hasMoreTokens()) {
                    String token = stringTokenizer.nextToken();
                    if (token.startsWith(":)") || token.equals("happy")){
                        //basic requirement @ execution 2
                    } else if (token.startsWith(":(") || token.equals("sad")){
                        //basic requirement @ execution 2                                               
                    }                                                                           
                }                                                            
            }            
        }
    }

    // TODO: if we don't need to produce multiple outputs, clean up the Reducer
    public static class Reduce extends MapReduceBase implements Reducer<Text, TheOutputClass, Text, TheOutputClass> {

        private int ordinal = 0;

        private MultipleOutputs out;

        @Override
        public void configure(JobConf job) {
            this.out = new MultipleOutputs(job);
        }

        public void reduce(Text key, Iterator<TheOutputClass> values, OutputCollector<Text, TheOutputClass> output, Reporter reporter) throws IOException {
            // TODO: reduce all values in the Iterator to one single TheOutputClass instance
        }

        @SuppressWarnings("unchecked")
        private <K, V> OutputCollector<K, V> getOutputCollector(Reporter reporter) throws IOException {
            return ((OutputCollector<K, V>) this.out.getCollector(SECOND_OUTPUT, reporter));
        }

        private void analyse(TheOutputClass tag, OutputCollector<TheOutputClass, Object> output) {
            // TODO: produce second output (if required)
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(Main.class);
        conf.setJobName("feels-analysis");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(TheOutputClass.class);
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setNumReduceTasks(1);
        conf.setInputFormat(CSVTextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        // TODO: determine whether we need extra output
        MultipleOutputs.addMultiNamedOutput(conf, SECOND_OUTPUT, TextOutputFormat.class, Text.class, TheOutputClass.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}