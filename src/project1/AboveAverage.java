package project1;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
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
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static java.lang.Boolean.TRUE;

/**
 * Created by Youqiao Ma on 1/30/2017.
 */

public class AboveAverage {

    public static class TotalCustMapper extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String next = value.toString();
            Customer c = new Customer(next);
            context.write(new Text("cust"), new Text("1"));
        }
    }
    public static class TotalTransMapper extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String next = value.toString();
            Transaction t = new Transaction(next);
            context.write(new Text("trans"), new Text("1"));
        }
    }

    public static class TotalReducer extends Reducer<Text, Text, Text, Text>{

        private Text resultvalue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> itr = values.iterator();
            int num = 0;

            while(itr.hasNext()){
                itr.next();
                num++;
            }
            resultvalue.set(String.valueOf(num));
            context.write(key, resultvalue);
        }
    }

    /**
     * mapper 1 is to deal with Customers file
     */
    public static class CustomersMapper extends Mapper<Object, Text, CompositeKeyWritable, Text>{

        private CompositeKeyWritable ckw = new CompositeKeyWritable();
        private int countCust = 0;
        private IntWritable id = new IntWritable();
        private Text text = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Consider to read each line first by using '\n'
            Customer c = new Customer(value.toString());
            text.set(c.name);
            ckw = new CompositeKeyWritable(c.id, 1);
            context.write(ckw, text);
            countCust++;
        }
    }

    /**
     * mapper 2 is to deal with Transactions file
     */
    public static class TransactionsMapper extends Mapper<Object, Text, CompositeKeyWritable, Text>{

        private CompositeKeyWritable ckw = new CompositeKeyWritable();

        private int countTrans = 0;

        private Text text = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Consider to read each line first by using '\n'
            Transaction t = new Transaction(value.toString());
            text.set("1");
            ckw = new CompositeKeyWritable(t.custID, 2);
            context.write(ckw, text);
            countTrans++;
        }
    }


    public static class CustTransReducer extends Reducer<CompositeKeyWritable,Text,IntWritable,Text> {

        private int totalCust = 0;
        private int totalTrans = 0;

        private int average = 0;

        private Text result = new Text();

        private IntWritable id = new IntWritable();

        public void reduce(CompositeKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            id.set(key.getjoinKey());
            Iterator<Text> itr = values.iterator();
            int numTransactions = 0;
            String user = itr.next().toString();

            while(itr.hasNext()){
                itr.next();
                numTransactions++;
            }
            if(numTransactions > average){
                result.set(user + " , " + numTransactions);
                context.write(id, result);
            }
        }

        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            if(cacheFilesLocal != null && cacheFilesLocal.length > 0){
                for (Path cache : cacheFilesLocal) {
                    loadAverage(cache);
                }
            }
            if(totalCust !=0 && totalTrans != 0){
                average = totalTrans / totalCust;
            }else{
                average = 100;
            }

        }

        public void loadAverage(Path filePath) throws IOException {
            String line = "";
            try{
                BufferedReader br = new BufferedReader(new FileReader(filePath.toString()));
                while ((line = br.readLine()) != null) {
                    String average[] = line.split("\t");
                    if(average[0].equals("cust")){
                        totalCust = Integer.parseInt(average[1]);
                    }else if(average[0].equals("trans")){
                        totalTrans = Integer.parseInt(average[1]);
                    }
                }
            }catch(FileNotFoundException e){
                System.out.println("FileNotFoundException");
                e.printStackTrace();
            }catch(IOException e) {
                System.out.println("IOException");
                e.printStackTrace();
            }
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
        String temp_file = "/user/hadoop/temp_file";
        Configuration conf = new Configuration();
        if (args.length != 3) {
            System.err.println("Usage: customer names who have more than average transactions <HDFS input file1> <HDFS input file2> <HDFS output file>");
            System.exit(2);
        }

        FileSystem  hdfs = FileSystem.get(conf);

        Job job1 = new Job(conf, "Job 1");
        job1.setJarByClass(AboveAverage.class);

        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, TotalCustMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, TotalTransMapper.class);
        FileOutputFormat.setOutputPath(job1, new Path(temp_file));
        job1.setReducerClass(TotalReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.waitForCompletion(true);

        Job job2 = new Job(conf, "Job 2");
        job2.setJarByClass(AboveAverage.class);

        MultipleInputs.addInputPath(job2, new Path(args[0]), TextInputFormat.class, CustomersMapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class, TransactionsMapper.class);
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        FileStatus[] fileList = hdfs.listStatus(new Path(temp_file) ,
                new PathFilter(){
                    @Override public boolean accept(Path path){
                        return path.getName().startsWith("part-");
                    }
                });
        for(int i=0; i < fileList.length;i++){
            DistributedCache.addCacheFile(fileList[i].getPath().toUri(), job2.getConfiguration());
        }
        //DistributedCache.addCacheFile((new Path(temp_file)).toUri(), job2.getConfiguration());

        job2.setMapOutputKeyClass(CompositeKeyWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setReducerClass(CustTransReducer.class);

        job2.setPartitionerClass(CustTransPartitioner.class);
        job2.setGroupingComparatorClass(CustTransComparator.class);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);

        job2.waitForCompletion(true);
        hdfs.delete(new Path(temp_file), TRUE);
        System.exit(0);

    }
}
