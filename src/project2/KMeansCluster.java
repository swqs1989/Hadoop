package project2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;
import util.Util;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Youqiao Ma on 2/23/2017.
 */
public class KMeansCluster {
    public static class KMeansMapper extends Mapper<Object, Text, Text, Text> {

        private HashMap<Point, Point> centroids = new HashMap<Point, Point>();

        private Text text_key = new Text();
        private Text text_value = new Text();

        private static int Iteration;

        protected void setup(Context context) throws IOException, InterruptedException {

            Iteration = Integer.parseInt(context.getConfiguration().get("Iteration", "0"));

            Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());

            for (Path eachPath : cacheFilesLocal) {
                if (eachPath.getName().toString().trim().equals("seeds") && Iteration == 0) {
                    loadCentroidHashMap(eachPath, context);
                }else if(eachPath.getName().startsWith("part-")){
                    loadCentroidHashMap(eachPath, context);
                }
            }

        }

        public void loadCentroidHashMap(Path filePath, Context context){
            String line = "";
            try{
                BufferedReader br = new BufferedReader(new FileReader(filePath.toString()));
                while ((line = br.readLine()) != null) {
                    String unit[] = line.split("\t");
                    centroids.put(new Point(unit[0], 1), new Point(unit[1], 1));
                }

            }catch(FileNotFoundException e){
                System.out.println("FileNotFoundException");
                e.printStackTrace();
            }catch(IOException e) {
                System.out.println("IOException");
                e.printStackTrace();
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String lines = value.toString();
            Point point = new Point(lines, 1);
            Point keyPoint = null;
            double min_dist = 0;
            for (Point pt: centroids.values()) {
                if (min_dist == 0) {
                    min_dist = calDistance(pt, point);
                    keyPoint = pt;
                }else{
                    if(min_dist > calDistance(pt, point)){
                        min_dist = calDistance(pt, point);
                        keyPoint = pt;
                    }
                }
            }
            text_key.set(keyPoint.toString());
            text_value.set(point.toString());
            context.write(text_key, text_value);
        }

        private double calDistance(Point p1, Point p2){
            return Math.sqrt(Math.pow(p1.getX_axis() - p2.getX_axis(), 2)+Math.pow(p1.getY_axis() - p2.getY_axis(), 2));
        }

    }


    public static class KMeansReducer extends Reducer<Text,Text,Text,Text> {
        private IntWritable result = new IntWritable();
        private Text text = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            double x_sum = 0, y_sum = 0;
            for(Text value: values){
                Point p = new Point(value.toString(), 1);
                x_sum += p.getX_axis();
                y_sum += p.getY_axis();
                ++count;
            }
            result.set(count);
            text.set(String.format("%.2f", x_sum / count) + "," + String.format("%.2f", y_sum / count));
            context.write(key, text);
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Count Flag among JSON File: <number of cluster> <HDFS input file> <HDFS output file>");
            System.exit(2);
        }
        int iteration = 0;
        boolean isFinished = false;
        while(iteration < 5 && !isFinished){
            Configuration conf = new Configuration();
            conf.set("K", args[0]);
            conf.set("Iteration", String.valueOf(iteration));

            FileSystem  hdfs = FileSystem.get(conf);

            Job job = new Job(conf, "KMeans job #" + iteration);
            job.setJarByClass(KMeansCluster.class);
            job.setMapperClass(KMeansMapper.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setReducerClass(KMeansReducer.class);

            job.setCombinerClass(KMeansReducer.class);

            job.setNumReduceTasks(1);

            // If it is the first iteration, the program will generate seeds file and add to distributed cache
            if(iteration == 0){
                genSeed(Integer.parseInt(args[0]), conf);
                DistributedCache.addCacheFile((new Path("/user/hadoop/seeds").toUri()), job.getConfiguration());
            }else{
                //Otherwise read the output file from last iteration
                FileStatus[] fileList = hdfs.listStatus(new Path(args[2] + "/temp" + (iteration - 1)) ,
                        new PathFilter(){
                            @Override public boolean accept(Path path){
                                return path.getName().startsWith("part-");
                            }
                        });
                for(int i=0; i < fileList.length;i++){
                    DistributedCache.addCacheFile(fileList[i].getPath().toUri(), job.getConfiguration());
                }
            }



            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2] + "/temp" + iteration));

            job.waitForCompletion(true);

            String outpath = args[2] + "/temp" + iteration;

            Path outputfile = new Path(outpath);
            FileSystem fs = FileSystem.get(new Configuration());

            //add
            hdfs = FileSystem.get(new Configuration());
            List<Point> pl1 = new ArrayList<>();
            List<Point> pl2 = new ArrayList<>();
            FileStatus[] fileList = hdfs.listStatus(new Path(outpath) ,
                    new PathFilter(){
                        @Override public boolean accept(Path path){
                            return path.getName().startsWith("part-");
                        }
                    });
            for(int i=0; i < fileList.length;i++){
                DistributedCache.addCacheFile(fileList[i].getPath().toUri(), new Configuration());
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fileList[i].getPath())));
                String temp = br.readLine();
                while (temp != null) {
                    String[] sp = temp.split("\t");
                    Point p1 = new Point(sp[0], 1);
                    Point p2 = new Point(sp[1], 1);
                    pl1.add(p1);
                    pl2.add(p2);
                    temp = br.readLine();
                }
            }
            //add end

            //something wrong with this
            //BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fileList)));

            for (int i = 0; i < pl1.size(); i++) {
                if((pl1.get(i).getY_axis() - pl2.get(i).getY_axis() < 100.0) &&
                        (pl1.get(i).getX_axis() - pl2.get(i).getX_axis() < 100.0)){
                    isFinished = true;
                }else{
                    isFinished = false;
                    break;
                }
            }
            iteration++;
        }


        System.exit(0);
    }

    public static void genSeed(int K, Configuration conf) throws Exception{
        FileSystem hdfs = FileSystem.get(conf);
        Path path = new Path("/user/hadoop/seeds");
        if ( hdfs.exists( path )) {
            hdfs.delete( path, true );
        }
        OutputStream os = hdfs.create( path,
                new Progressable() {
                    public void progress() {
                    } });
        BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
        for (int i = 0; i < K; i++) {
            String points = Util.getPointLine(0, 10000);
            br.write(points + "\t" + points);
            br.write("\n");
        }
        br.close();
        hdfs.close();
    }
}
