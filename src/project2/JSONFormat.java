package project2;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 * Created by Youqiao Ma on 2/22/2017.
 */
public class JSONFormat extends TextInputFormat {
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context){
        return new JSONReader();
    }
}
