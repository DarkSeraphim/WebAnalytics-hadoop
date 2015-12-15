package net.darkseraphim.webanalytics.hadoop.statistics;

import net.darkseraphim.webanalytics.hadoop.csv.CSVTextInputFormat;
import net.darkseraphim.webanalytics.hadoop.csv.Row;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

import java.io.IOException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * WordCount sample from their tutorial
 */
public class Main
{
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Row, Text, HashTag> {
        private static final SimpleDateFormat sdf = new SimpleDateFormat("");
        private Text tag = new Text();

        public void map(LongWritable key, Row value, OutputCollector<Text, HashTag> output, Reporter reporter) throws IOException {
            String date = value.get(5).toString();
            String line = value.get(4).toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                if (token.startsWith("#")) {
                    tag.set(token.substring(1));
                    HashTag hashTag = new HashTag(tag.toString());
                    output.collect(tag, hashTag);
                    hashTag.setDate(sdf.parse(date, new ParsePosition(0)));
                }
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, HashTag, HashTag, Text> {

        private int ordinal = 0;

        private MultipleOutputs out;

        @Override
        public void configure(JobConf job) {
            this.out = new MultipleOutputs(job);
        }

        public void reduce(Text key, Iterator<HashTag> values, OutputCollector<HashTag, Text> output, Reporter reporter) throws IOException {
            HashTag tag = new HashTag(key.toString());
            while (values.hasNext()) {
                tag.merge(values.next());
            }
            tag.setRank(this.ordinal++);
            if (tag.getRank() < 10) {
                analyse(tag, this.<HashTag, Object>getOutputCollector(reporter));
            }
            output.collect(tag, key);
        }

        @SuppressWarnings("unchecked")
        private <K, V> OutputCollector<K, V> getOutputCollector(Reporter reporter) throws IOException {
            return ((OutputCollector<K, V>) this.out.getCollector("top10", reporter));
        }

        private void analyse(HashTag tag, OutputCollector<HashTag, Object> output) {

        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(Main.class);
        conf.setJobName("hashtag-analysis");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(HashTag.class);
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setNumReduceTasks(1);
        conf.setInputFormat(CSVTextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        MultipleOutputs.addMultiNamedOutput(conf, "top10", TextOutputFormat.class, Text.class, HashTag.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);

        /*JobControl control = new JobControl("hashtag-analysis");
        Job countJob = new Job(conf);
        control.addJob(countJob);
        ArrayList<Job> dependencies = new ArrayList<Job>();
        dependencies.add(countJob);
        control.addJob(new Job(conf, dependencies));
        control.run();*/
    }
}