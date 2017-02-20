package project1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by Youqiao Ma on 1/30/2017.
 */

public class CustSumTrans {

    public static class CustomersMapper extends Mapper<Object, Text, IntWritable, Text>{

        private IntWritable id = new IntWritable();
        private Text text = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Consider to read each line first by using '\n'
            Transaction t = new Transaction(value.toString());
            text.set("1" + "," + t.transTotal);
            id.set(t.custID);
            context.write(id, text);
        }
    }


    public static class CustomersReducer extends Reducer<IntWritable,Text,IntWritable,Text> {

        private Text result = new Text();

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int numTransactions = 0;
            double totalSum = 0;
            for (Text val : values) {
                String[] temp = val.toString().split(",");
                numTransactions += Integer.parseInt(temp[0]);
                totalSum += Double.parseDouble(temp[1]);
            }
            result.set(numTransactions + "," + String.format("%.2f", totalSum));
            context.write(key, result);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: total sum of transactions for every customer <HDFS input file> <HDFS output file>");
            System.exit(2);
        }
        Job job = new Job(conf, "Query2");
        job.setJarByClass(CustSumTrans.class);
        job.setMapperClass(CustomersMapper.class);
        job.setCombinerClass(CustomersReducer.class);
        job.setReducerClass(CustomersReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
