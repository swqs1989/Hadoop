package project1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by Youqiao Ma on 1/30/2017.
 */

public class CustTrans {

    /**
    * mapper 1 is to deal with Customers file
    */
    public static class CustomersMapper extends Mapper<Object, Text, CompositeKeyWritable, Text>{

        private CompositeKeyWritable ckw = new CompositeKeyWritable();

        private IntWritable id = new IntWritable();
        private Text text = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Consider to read each line first by using '\n'
            Customer c = new Customer(value.toString());
            text.set(c.name+ "," + c.salary);
            ckw = new CompositeKeyWritable(c.id, 1);
            context.write(ckw, text);
        }
    }

    /**
    * mapper 2 is to deal with Transactions file
    */
    public static class TransactionsMapper extends Mapper<Object, Text, CompositeKeyWritable, Text>{

        private CompositeKeyWritable ckw = new CompositeKeyWritable();

        private Text text = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Consider to read each line first by using '\n'
            Transaction t = new Transaction(value.toString());
            text.set(t.transNumItems + "," + t.transTotal);
            ckw = new CompositeKeyWritable(t.custID, 2);
            context.write(ckw, text);
        }
    }


    public static class CustTransReducer extends Reducer<CompositeKeyWritable,Text,IntWritable,Text> {

        private Text result = new Text();

        private IntWritable id = new IntWritable();

        public void reduce(CompositeKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            id.set(key.getjoinKey());
            Iterator<Text> itr = values.iterator();
            int numTransactions = 0;
            double totalSum = 0;
            int minItems = 0;
            String user = itr.next().toString();
            while(itr.hasNext()){
                String[] temp = itr.next().toString().split(",");
                int items = Integer.parseInt(temp[0]);
                totalSum += Double.parseDouble(temp[1]);
                if(minItems == 0){
                    minItems = items;
                }else{
                    minItems = Math.min(minItems, items);
                }
                numTransactions++;
            }

            result.set(user + "," + numTransactions + "," + String.format("%.2f", totalSum) + "," + minItems);
            context.write(id, result);
        }
    }

    public static class CustTransPartitioner extends Partitioner<CompositeKeyWritable, Text> {
    	public int getPartition(CompositeKeyWritable key, Text value, int numReduceTasks) {
    		return (key.hashCode() % numReduceTasks);
    	}
    }

    public static class CustTransComparator extends WritableComparator {

    	protected CustTransComparator() {
    		super(CompositeKeyWritable.class, true);
    	}

    	@Override
    	public int compare(WritableComparable w1, WritableComparable w2) {
    		// Sort on all attributes of composite key
    		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
    		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;

    		return CompositeKeyWritable.compare(key1.getjoinKey(), key2.getjoinKey());
    	}
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 3) {
            System.err.println("Usage: total sum of transactions for every customer <HDFS input file1> <HDFS input file2> <HDFS output file>");
            System.exit(2);
        }
        Job job = new Job(conf, "Query3");
        job.setJarByClass(CustTrans.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomersMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransactionsMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setMapOutputKeyClass(CompositeKeyWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(CustTransReducer.class);

        job.setPartitionerClass(CustTransPartitioner.class);
        job.setGroupingComparatorClass(CustTransComparator.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
