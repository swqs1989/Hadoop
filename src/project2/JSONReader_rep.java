package project2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.LineReader;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * Created by Youqiao Ma on 2/20/2017.
 */
public class JSONReader_rep extends RecordReader<LongWritable, Text> {
    private final int LINES_TO_READ = 15;
    private long start;
    private long pos;
    private long end;

    private LineReader in;

    private int maxLineLength;
    private LongWritable key = new LongWritable();
    private Text text = new Text();

    FSDataInputStream fileIn;

    private final DataOutputBuffer buffer = new DataOutputBuffer();

    private byte[] startTag = "{".getBytes();
    private byte[] endTag = "}".getBytes();

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;

        Configuration conf = context.getConfiguration();

        this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);

        start = split.getStart();
        end = start + split.getLength();

        Path file = split.getPath();
        FileSystem fs = file.getFileSystem(conf);
        fileIn = fs.open(split.getPath());
        fileIn.seek(start);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (fileIn.getPos() < end) {
            if (isJSON(startTag, false)) {
                try {
                    buffer.write(startTag);
                    if (isJSON(endTag, true)) {
                        key.set(fileIn.getPos());
                        text.set(buffer.getData(), 0,
                                buffer.getLength());
                        return true;
                    }
                } finally {
                    buffer.reset();
                }
            }
        }
        return false;
    }

    private boolean isJSON(byte[] match, boolean withinBlock) throws IOException {
        int i = 0;
        while (true) {
            int b = fileIn.read();
            // end of file:
            if (b == -1)
                return false;
            // save to buffer:
            if (withinBlock)
                buffer.write(b);
            // check if we're matching:
            if (b == match[i]) {
                i++;
                if (i >= match.length)
                    return true;
            } else
                i = 0;
            // see if we've passed the stop point:
            if (!withinBlock && i == 0 && fileIn.getPos() >= end)
                return false;
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return text;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if(start == end){
            return 0.0f;
        }else{
            return Math.min(1.0f, (pos - start)/ (float) (end - start));
        }
    }

    @Override
    public void close() throws IOException {
        if (in != null){
            in.close();
        }
    }
}
