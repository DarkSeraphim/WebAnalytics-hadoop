package net.darkseraphim.webanalytics.hadoop.statistics;

import net.darkseraphim.webanalytics.hadoop.csv.CSVTextInputFormat;
import net.darkseraphim.webanalytics.hadoop.csv.Row;
import net.darkseraphim.webanalytics.hadoop.statistics.io.WritableInputFormat;
import net.darkseraphim.webanalytics.hadoop.statistics.io.WritableOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

import java.io.IOException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
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

        @Override
        public void configure(JobConf job) {
            super.configure(job);
        }

        private int lastPerc = -1;

        public void map(LongWritable key, Row value, OutputCollector<Text, HashTag> output, Reporter reporter) throws IOException {
            String date = value.get(5).toString();
            String line = value.get(4).toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                if (token.startsWith("#") && token.length() > 1) {
                    tag.set(token.substring(1));
                    HashTag hashTag = new HashTag(tag.toString());
                    hashTag.setDate(sdf.parse(date, new ParsePosition(0)));
                    output.collect(tag, hashTag);
                }
            }
            if (CSVTextInputFormat.reader == null) {
                if (this.lastPerc < 0) {
                    System.out.println("No reader found");
                    this.lastPerc = 0;
                }
                return;
            }
            int perc = (int) Math.ceil(CSVTextInputFormat.reader.getProgress() * 100);
            if (perc > lastPerc) {
                lastPerc = perc;
                System.out.println(perc + "% mapped (" + CSVTextInputFormat.reader.getPos() + " / " + CSVTextInputFormat.reader.getEnd() + ")");
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, HashTag, HashTag, IntWritable> {

        private MultipleOutputs out;

        private IntWritable num = new IntWritable();

        @Override
        public void configure(JobConf job) {
            this.out = new MultipleOutputs(job);
        }

        public void reduce(Text key, Iterator<HashTag> values, OutputCollector<HashTag, IntWritable> output, Reporter reporter) throws IOException {
            HashTag tag = new HashTag(key.toString());
            while (values.hasNext()) {
                tag.merge(values.next());
            }
            this.num.set(tag.getCount());
            analyse(tag, this.<HashTag, IntWritable>getOutputCollector("rawdata", reporter));
            output.collect(tag, this.num);
        }

        @SuppressWarnings("unchecked")
        private <K, V> OutputCollector<K, V> getOutputCollector(String name, Reporter reporter) throws IOException {
            return ((OutputCollector<K, V>) this.out.getCollector(name, reporter));
        }

        private void analyse(HashTag tag, OutputCollector<HashTag, IntWritable> output) throws IOException {
            this.num.set(tag.getCount());
            output.collect(tag, this.num);
        }

        @Override
        public void close() throws IOException {
            try {
                this.out.close();
            } finally {
                super.close();
            }
        }
    }

    public static class Filter extends MapReduceBase implements Reducer<HashTag, IntWritable, HashTag, IntWritable> {

        private int rank = 0;

        private IntWritable num = new IntWritable();

        public void reduce(HashTag key, Iterator<IntWritable> values, OutputCollector<HashTag, IntWritable> output, Reporter reporter) throws IOException {
            if (++this.rank <= 10) {
                this.num.set(key.getCount());
                output.collect(key, this.num);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(Main.class);
        conf.setJobName("hashtag-count");

        conf.setOutputKeyClass(HashTag.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(Map.class);
        conf.setMapOutputKeyClass(Text.class);
        WritableInputFormat.setInputKeyClass(conf, HashTag.class);
        WritableInputFormat.setInputValueClass(conf, IntWritable.class);
        conf.setMapOutputValueClass(HashTag.class);
        conf.setReducerClass(Reduce.class);

        conf.setNumReduceTasks(1);

        conf.setInputFormat(CSVTextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        MultipleOutputs.addNamedOutput(conf, "rawdata", WritableOutputFormat.class, HashTag.class, IntWritable.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        conf.set("io.compression.codecs", GzipCodec.class.getName() + "," + DefaultCodec.class.getName());

        System.out.println("[LOG] Running job...");
        // Uncomment & remove futher code for single jobs
        // JobClient.runJob(conf);


        JobConf topConf = new JobConf(Main.class);
        topConf.setJobName("hashtag-top");

        topConf.setOutputKeyClass(HashTag.class);
        topConf.setOutputValueClass(IntWritable.class);
        topConf.setMapperClass(IdentityMapper.class);
        topConf.setMapOutputKeyClass(HashTag.class);
        WritableInputFormat.setInputKeyClass(topConf, HashTag.class);
        WritableInputFormat.setInputValueClass(topConf, IntWritable.class);
        topConf.setMapOutputValueClass(IntWritable.class);
        topConf.setReducerClass(Filter.class);

        topConf.setNumReduceTasks(1);

        topConf.setInputFormat(WritableInputFormat.class);
        topConf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(topConf, new Path(args[1], "rawdata-r-00000"));
        FileOutputFormat.setOutputPath(topConf, new Path(args[2]));
        topConf.set("io.compression.codecs", GzipCodec.class.getName() + "," + DefaultCodec.class.getName());

        JobControl control = new JobControl("hashtag-analysis");
        Job countJob = new Job(conf);
        ArrayList<Job> dependencies = new ArrayList<Job>();
        dependencies.add(countJob);
        control.addJobs(Arrays.asList(countJob, new Job(topConf, dependencies)));
        new Thread(control).start();
        while (!control.allFinished()) {
            try {
                Thread.sleep(30000);
            } catch (InterruptedException ex) {
            }
        }
        System.exit(0);
    }
}