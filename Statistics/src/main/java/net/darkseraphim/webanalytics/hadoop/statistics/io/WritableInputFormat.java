package net.darkseraphim.webanalytics.hadoop.statistics.io;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.*;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.nio.file.FileSystem;
import java.nio.file.Path;

/**
 * @author DarkSeraphim.
 */
public class WritableInputFormat<K extends Writable, V extends Writable> extends FileInputFormat<K, V>  {

    private CompressionCodecFactory compressionCodecs = null;

    public WritableInputFormat() {
    }

    public void configure(JobConf conf) {
        this.compressionCodecs = new CompressionCodecFactory(conf);
    }

    protected boolean isSplitable(FileSystem fs, Path file) {
        return false;
    }

    public WritableRecordReader<K, V> getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter) throws IOException {
        reporter.setStatus(genericSplit.toString());
        return new WritableRecordReader<K, V>(job, (FileSplit) genericSplit);
    }

    public static void setInputKeyClass(JobConf job, Class<? extends Writable> cls) {
        job.setClass("mapred.wif.key", cls, Writable.class);
    }

    public static void setInputValueClass(JobConf job, Class<? extends Writable> cls) {
        job.setClass("mapred.wif.value", cls, Writable.class);
    }

    public static Class<? extends Writable> getInputKeyClass(JobConf job) {
        return job.getClass("mapred.wif.key", null, Writable.class);
    }

    public static Class<? extends Writable> getInputValueClass(JobConf job) {
        return job.getClass("mapred.wif.value", null, Writable.class);
    }

    @SuppressWarnings("unchecked")
    private static class WritableRecordReader<K extends Writable, V extends Writable> implements RecordReader<K, V> {

        private class CountingInputStream extends InputStream {

            private final InputStream delegate;

            private long pos = 0;

            private long mark;

            CountingInputStream(InputStream delegate, long start) {
                this.delegate = delegate;
                this.pos = start;
            }

            @Override
            public int read() throws IOException {
                int ret = this.delegate.read();
                // Ensure the byte is read before incrementing pos
                this.pos++;
                return ret;
            }

            @Override
            public int read(byte[] b) throws IOException {
                int ret;
                this.pos += ret = this.delegate.read(b);
                return ret;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                int ret;
                this.pos += ret = this.delegate.read(b, off, len);
                return ret;
            }

            @Override
            public long skip(long n) throws IOException {
                long ret;
                this.pos += ret = this.delegate.skip(n);
                return ret;
            }

            @Override
            public int available() throws IOException {
                return this.delegate.available();
            }

            @Override
            public void close() throws IOException {
                this.delegate.close();
                this.pos = -1;
            }

            @Override
            public void mark(int readlimit) {
                this.delegate.mark(readlimit);
                this.mark = this.pos;
            }

            @Override
            public void reset() throws IOException {
                this.delegate.reset();
                this.pos = this.mark;
            }

            @Override
            public boolean markSupported() {
                return this.delegate.markSupported();
            }

            public long getPos() {
                return this.pos;
            }
        }

        private JobConf job;

        private CountingInputStream cin;

        private DataInputStream in;

        private long start;

        private long end;

        private CompressionCodecFactory compressionCodecs;

        public WritableRecordReader(JobConf job, FileSplit split) {
            this.job = job;
            try {
                initialize(split, job);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        private void initialize(FileSplit split, JobConf conf) throws IOException{
            start = split.getStart();
            end = start + split.getLength();
            final org.apache.hadoop.fs.Path file = split.getPath();
            compressionCodecs = new CompressionCodecFactory(conf);
            final CompressionCodec codec = compressionCodecs.getCodec(file);

            // open the file and seek to the start of the split
            org.apache.hadoop.fs.FileSystem fs = file.getFileSystem(conf);
            FSDataInputStream fileIn = fs.open(split.getPath());

            InputStream is;
            if (codec != null) {
                is = codec.createInputStream(fileIn);
                end = Long.MAX_VALUE;
            } else {
                if (start != 0) {
                    fileIn.seek(start);
                }
                is = fileIn;
            }

            is = this.cin = new CountingInputStream(is, start);
            this.in = new DataInputStream(is);
        }

        public boolean next(Writable key, Writable value) throws IOException {
            try {
                key.readFields(this.in);
                value.readFields(this.in);
            } catch (EOFException ex) {
                return false;
            }
            return true;
        }

        public K createKey() {
            try {
                Constructor<?> constr = getInputKeyClass(this.job).getDeclaredConstructor(new Class[0]);
                constr.setAccessible(true);
                return (K) constr.newInstance();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        public V createValue() {
            try {
                Constructor<?> constr = getInputValueClass(this.job).getDeclaredConstructor(new Class[0]);
                constr.setAccessible(true);
                return (V) constr.newInstance();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        public long getPos() throws IOException {
            return this.cin.getPos();
        }

        public void close() throws IOException {
            if (this.in != null) {
                this.in.close();
            }
        }

        public float getProgress() {
            if (start == end) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (this.cin.getPos() - start) / (float) (end - start));
            }
        }
    }
}
