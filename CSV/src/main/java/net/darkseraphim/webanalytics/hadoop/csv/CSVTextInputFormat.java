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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
public class CSVTextInputFormat extends FileInputFormat<LongWritable, List<Text>>
{

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.InputFormat#createRecordReader(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public RecordReader<LongWritable, List<Text>> getRecordReader(InputSplit split, JobConf conf, Reporter reporter)
            throws IOException {
        String quote = conf.get(CSVLineRecordReader.FORMAT_DELIMITER);
        String separator = conf.get(CSVLineRecordReader.FORMAT_SEPARATOR);
        if (null == quote || null == separator) {
            throw new IOException("CSVTextInputFormat: missing parameter delimiter/separator");
        }
        return new CSVLineRecordReader();
    }
}