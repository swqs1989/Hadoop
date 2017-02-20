package project2;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

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

public class SpatialJoin {

    /**
     * mapper 1 is to deal with Customers file
     */
    public static class PointsMapper extends Mapper<Object, Text, PRKey, Text>{

        private PRKey prk = new PRKey();

        private IntWritable id = new IntWritable();
        private Text text = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Consider to read each line first by using '\n'
            Point point = new Point(value.toString());
            text.set(point.toString());
            prk = new PRKey(Integer.parseInt(point.location), 1);
            context.write(prk, text);
        }
    }

    /**
     * mapper 2 is to deal with Transactions file
     */
    public static class RectanglesMapper extends Mapper<Object, Text, PRKey, Text>{

        private PRKey prk = new PRKey();

        private Text text = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            Rectangle rect = new Rectangle(value.toString());
            text.set(rect.toString());
            for(String area : rect.area){
                prk = new PRKey(Integer.parseInt(area), 2);
                context.write(prk, text);
            }
        }
    }


    public static class PRReducer extends Reducer<PRKey,Text,Text,Text> {

        private Text rkey = new Text();
        private Text rvalue = new Text();

        private IntWritable id = new IntWritable();

        public void reduce(PRKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            LinkedList<Point> pll = new LinkedList();
            Iterator<Text> itr = values.iterator();
            String user = itr.next().toString();
            while(itr.hasNext()){
                String temp = itr.next().toString();
                try{
                    Point p = new Point(temp);
                    pll.add(p);
                }catch (Exception e){
                    Rectangle rec = new Rectangle(temp);
                    for(Point p : pll){
                        if(rec.isInside(p)){
                            rkey.set(rec.getName());
                            rvalue.set(p.toString());
                            context.write(rkey, rvalue);
                        }
                    }
                    break;
                }
            }

            while(itr.hasNext()){
                Rectangle rec = new Rectangle(itr.next().toString());
                for(Point p : pll){
                    if(rec.isInside(p)){
                        rkey.set(rec.getName());
                        rvalue.set(p.toString());
                        context.write(rkey, rvalue);
                    }
                }
            }
        }
    }

    public static class PRPartitioner extends Partitioner<PRKey, Text> {
        public int getPartition(PRKey key, Text value, int numReduceTasks) {
            return (key.hashCode() % numReduceTasks);
        }
    }

    public static class PRComparator extends WritableComparator {

        protected PRComparator() {
            super(PRKey.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            // Sort on all attributes of composite key
            PRKey key1 = (PRKey) w1;
            PRKey key2 = (PRKey) w2;

            return PRKey.compare(key1.getjoinKey(), key2.getjoinKey());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 3) {
            System.err.println("Usage: Spatial Join <HDFS input file1> <HDFS input file2> <HDFS output file>");
            System.exit(2);
        }
        Job job = new Job(conf, "SpatialJoin");
        job.setJarByClass(SpatialJoin.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PointsMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RectanglesMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setMapOutputKeyClass(PRKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(5);

        job.setReducerClass(PRReducer.class);

        job.setPartitionerClass(PRPartitioner.class);
        job.setGroupingComparatorClass(PRComparator.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
