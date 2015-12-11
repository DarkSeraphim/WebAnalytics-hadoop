package net.darkseraphim.webanalytics.hadoop.statistics;

import net.darkseraphim.webanalytics.hadoop.csv.CSVTextInputFormat;
import net.darkseraphim.webanalytics.hadoop.csv.Row;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

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
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Row, Text, HashTag>
    {
        private static final SimpleDateFormat sdf = new SimpleDateFormat("");
        private Text tag = new Text();
        private HashTag hashTag = new HashTag();

        public void map(LongWritable key, Row value, OutputCollector<Text, HashTag> output, Reporter reporter) throws IOException
        {
            String date = value.get(5).toString();
            hashTag.setDate(sdf.parse(date, new ParsePosition(0)));
            String line = value.get(4).toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                if (token.startsWith("#")) {
                    tag.set(token.substring(1));
                    output.collect(tag, hashTag);
                }
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, HashTag, Text, HashTag>
    {
        public void reduce(Text key, Iterator<HashTag> values, OutputCollector<Text, HashTag> output, Reporter reporter) throws IOException {
            HashTag tag = new HashTag();
            while (values.hasNext()) {
                tag.merge(values.next());
            }
            output.collect(key, tag);
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

        conf.setInputFormat(CSVTextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
    }
}