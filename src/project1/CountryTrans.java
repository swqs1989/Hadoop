package project1;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by Youqiao Ma on 1/30/2017.
 */

public class CountryTrans  {

    /**
     * mapper 1 is to deal with Customers file
     */
    public static class TransMapper extends Mapper<Object, Text, IntWritable, DoubleWritable>{

        private HashMap<Integer, Integer> idCode = new HashMap<Integer, Integer>();
        private IntWritable ccode = new IntWritable();
        private DoubleWritable total = new DoubleWritable();

        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());

            for (Path eachPath : cacheFilesLocal) {
                if (eachPath.getName().toString().trim().equals("Customers")) {
                    loadCustomersHashMap(eachPath, context);
                }
            }

        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Consider to read each line first by using '\n'
            Transaction t = new Transaction(value.toString());
            int country;
            country = idCode.get(t.custID);

            ccode.set(country);
            total.set(t.transTotal);
            context.write(ccode, total);
        }



        /**
         * Since the customer file should be much smaller than transaction file.
         * It is feasible that load customer file in cache and do map only job.
         */
        public void loadCustomersHashMap(Path filePath, Context context) throws IOException {
            String line = "";
            try{
                BufferedReader br = new BufferedReader(new FileReader(filePath.toString()));
                while ((line = br.readLine()) != null) {
                    String customers[] = line.split(",");
                    System.out.println(customers);
                    idCode.put(Integer.parseInt(customers[0]), Integer.parseInt(customers[3]));
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

    public static class CountryTransReducer extends Reducer<IntWritable,DoubleWritable,IntWritable,Text> {

        private Text result = new Text();

        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double maxTotal = 0.0;
            double minTotal = 0.0;
            int sumCust = 0;
            for (DoubleWritable val : values) {
                if(maxTotal == 0 || minTotal == 0){
                    maxTotal = val.get();
                    minTotal = val.get();
                }
                maxTotal = Math.max(maxTotal, val.get());
                minTotal = Math.min(minTotal, val.get());
                sumCust++;
            }
            result.set(sumCust + "," + String.format("%.2f", minTotal) + "," + String.format("%.2f", maxTotal));
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: query 4 <HDFS input file1> <HDFS output file>");
            System.exit(2);
        }

        Job job = new Job(conf, "query4");
        DistributedCache.addCacheFile((new Path("/user/hadoop/data/Customers")).toUri(), job.getConfiguration());

        job.setJarByClass(CountryTrans.class);
        job.setMapperClass(TransMapper.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setNumReduceTasks(2);

        job.setReducerClass(CountryTransReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
