package project2;
/**
 * Created by Youqiao Ma on 2/22/2017.
 * check github function
 */
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JSONJob {

    public static class JSONMapper extends Mapper<LongWritable, Text, Text, Text>{

        private final static IntWritable one = new IntWritable(1);
        private Text okey = new Text();
        private Text ovalue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lines = value.toString();
            HashMap<String, String> hm = JSONFrame.parseJSON(lines);
            okey.set(hm.get("Flags"));
            ovalue.set("1");
            context.write(okey, ovalue);
        }
    }


    public static class JSONReducer extends Reducer<Text,Text,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(Text value: values){
                ++count;
            }
            result.set(count);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Count Flag among JSON File: <HDFS input file> <HDFS output file>");
            System.exit(2);
        }

        FileSystem hdfs = FileSystem.get(conf);
        Path path = new Path(args[0]);
        long length = hdfs.getFileStatus(path).getLen();
        hdfs.close();
        conf.setLong("mapred.max.split.size", length / 5);

        Job job = new Job(conf, "JSON job");
        job.setJarByClass(JSONJob.class);
        job.setMapperClass(JSONMapper.class);
        job.setReducerClass(JSONReducer.class);
        job.setInputFormatClass(JSONFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setNumReduceTasks(2);
        //job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}