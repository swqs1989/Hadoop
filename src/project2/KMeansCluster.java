package project2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/**
 * Created by Youqiao Ma on 2/23/2017.
 */
public class KMeansCluster {
    public static class PointsMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        private HashMap<Integer, Point> centroids = new HashMap<Integer, Point>();

        private final static IntWritable one = new IntWritable(1);
        private Text intw = new Text();
        private Text text = new Text();

        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());

            for (Path eachPath : cacheFilesLocal) {
                if (eachPath.getName().toString().trim().equals("Customers")) {
                    loadCentroidHashMap(eachPath, context);
                }
            }

        }

        public void loadCentroidHashMap(Path filePath, Context context){
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

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lines = value.toString();
            HashMap<String, String> hm = JSONFrame.parseJSON(lines);
            okey.set(hm.get("Flags"));
            ovalue.set("1");
            context.write(okey, ovalue);
        }


    }


    public static class PointsReducer extends Reducer<IntWritable,Text,IntWritable,Text> {
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
        Job job = new Job(conf, "JSON job");
        job.setJarByClass(KMeansCluster.class);
        job.setMapperClass(PointsMapper.class);
        job.setReducerClass(PointsReducer.class);
        job.setInputFormatClass(JSONFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setNumReduceTasks(2);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        DistributedCache.addCacheFile((new Path("/user/hadoop/cache/Customers")).toUri(), job.getConfiguration());
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
