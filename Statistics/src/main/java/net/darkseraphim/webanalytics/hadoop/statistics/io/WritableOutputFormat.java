package net.darkseraphim.webanalytics.hadoop.statistics.io;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author DarkSeraphim.
 */
public class WritableOutputFormat extends FileOutputFormat<Writable, Writable> {
    @Override
    public RecordWriter<Writable, Writable> getRecordWriter(FileSystem fileSystem, JobConf job, String name, Progressable progress) throws IOException {
        boolean isCompressed = getCompressOutput(job);
        String keyValueSeparator = job.get("mapreduce.output.textoutputformat.separator", "\t");
        if(!isCompressed) {
            Path codecClass = FileOutputFormat.getTaskOutputPath(job, name);
            FileSystem codec = codecClass.getFileSystem(job);
            FSDataOutputStream file = codec.create(codecClass, progress);
            return new WritableRecordWriter(file);
        } else {
            Class codecClass = getOutputCompressorClass(job, GzipCodec.class);
            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, job);
            Path file = FileOutputFormat.getTaskOutputPath(job, name + codec.getDefaultExtension());
            FileSystem fs = file.getFileSystem(job);
            FSDataOutputStream fileOut = fs.create(file, progress);
            return new WritableRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)));
        }
    }

    private static class WritableRecordWriter implements RecordWriter<Writable, Writable> {

        protected DataOutputStream out;

        public WritableRecordWriter(DataOutputStream out) {
            this.out = out;
        }

        public synchronized void write(Writable key, Writable value) throws IOException {
            key.write(this.out);
            value.write(this.out);
        }

        public synchronized void close(Reporter reporter) throws IOException {
            this.out.close();
        }
    }
}
