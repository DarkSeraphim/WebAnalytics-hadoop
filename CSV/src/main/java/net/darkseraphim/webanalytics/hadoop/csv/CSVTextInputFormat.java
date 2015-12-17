/**
 * Copyright 2014 Marcelo Elias Del Valle
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.darkseraphim.webanalytics.hadoop.csv;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.List;

;

/**
 * Configurable CSV line reader. Variant of TextInputReader that reads CSV
 * lines, even if the CSV has multiple lines inside a single column
 *
 *
 * @author mvallebr
 *
 */
public class CSVTextInputFormat extends FileInputFormat<LongWritable, List<Text>> implements JobConfigurable {

    public static CSVLineRecordReader reader;

    private CompressionCodecFactory compressionCodecs;

    public void configure(JobConf conf) {
        this.compressionCodecs = new CompressionCodecFactory(conf);
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException
    {
        InputSplit[] splits = super.getSplits(job, numSplits);
        System.out.println("[LOG] getSplits called, " + splits.length + " splits created");
        return splits;
    }

    public boolean isSplitable(FileSystem fs, Path file) {
        CompressionCodec codec = this.compressionCodecs.getCodec(file);
        System.out.println("[LOG] isSplitable called for " + file.getName() + ", " + false + " returned");
        return false;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.InputFormat#createRecordReader(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public RecordReader<LongWritable, List<Text>> getRecordReader(InputSplit split, JobConf conf, Reporter reporter)
            throws IOException {
        String quote = conf.get(CSVLineRecordReader.FORMAT_DELIMITER);
        String separator = conf.get(CSVLineRecordReader.FORMAT_SEPARATOR);
        conf.set(CSVLineRecordReader.FORMAT_DELIMITER, CSVLineRecordReader.DEFAULT_DELIMITER);
        conf.set(CSVLineRecordReader.FORMAT_SEPARATOR, CSVLineRecordReader.DEFAULT_SEPARATOR);
        conf.setBoolean(CSVLineRecordReader.IS_ZIPFILE, false);
        System.out.println("[LOG] Created reader");
        if (split instanceof FileSplit) {
            return reader = new CSVLineRecordReader(split, conf);
        }
        throw new UnsupportedOperationException("Only FileSplits are supported");
    }
}