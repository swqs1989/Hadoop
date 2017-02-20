package project1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by Youqiao Ma on 1/30/2017.
 */

public class QueryCustomers {

    public static class CustomersMapper extends Mapper<Object, Text, Text, NullWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text text = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Consider to read each line first by using '\n'
            /*
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                Customer c = new Customer(itr.nextToken());
                if (c.countrycode >= 2 && c.countrycode <= 6){
                	text.set(c.getValueString());
                	context.write(text, null);
                }
            }
             */
            Customer c = new Customer(value.toString());
            if (c.countrycode >= 2 && c.countrycode <= 6){
                text.set(c.id + "," + c.getValueString());
                context.write(text, null);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: customers whose CountryCode between 2 and 6 <HDFS input file> <HDFS output file>");
            System.exit(2);
        }
        Job job = new Job(conf, "Query1");
        job.setJarByClass(QueryCustomers.class);
        job.setMapperClass(CustomersMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setNumReduceTasks(0);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
