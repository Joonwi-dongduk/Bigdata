import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class YouTubeStudent20190996 {
        public static class Youtube {
                public String category;
                public double rate;

                public Youtube (String category, double rate) {
                        this.category = category;
                        this.rate = rate;
                }
                public String getString() {
                        return category + "|" + rate;
                }
        }

        public static class YoutubeComparator implements Comparator<Youtube> {
                public int compare(Youtube y1, Youtube y2) {
                        if (y1.rate > y2.rate)
                                return 1;
                        else if (y1.rate < y2.rate)
                                return -1;
                        return 0;
                }
        }

        public static void insertTop(PriorityQueue q, String category, double rate, int topK) {
                Youtube youtube = (Youtube)q.peek();
                if (q.size() < topK || youtube.rate < rate) {
                        Youtube y = new Youtube(category, rate);
                        q.add(y);
                        if (q.size() > topK)
                                q.remove();
                }
        }



        public static class YouTubeStudent20190996Mapper extends Mapper<Object, Text, Text, DoubleWritable> {
                //private Text outputKey = new Text();
                //private Text outputValue = new Text();

                public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                        String[] str = value.toString().split("|");

                        //outputKey.set(str[3]);
                        //outputValue.set(str[6]);
                        context.write(new Text(str[3]), new DoubleWritable(Double.parseDouble(str[6])));
                }
        }

        public static class YouTubeStudent20190996Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

                private DoubleWritable reduceValue = new DoubleWritable();
                public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
                        double total = 0;
                        double avgRate;
                        int len = 0;

                        for (DoubleWritable val : values) {
                                total += val.get();
                                len++;
                        }
                        avgRate = total / (double)len;
                        reduceValue.set(avgRate);

                        context.write(new Text(key.toString() + "::"), reduceValue);
                }
        }
        public static class TopKMapper2 extends Mapper<Object, Text, Text, NullWritable> {
                private Comparator<Youtube> comp = new YoutubeComparator();
                private PriorityQueue<Youtube> queue = new PriorityQueue<Youtube>(10, comp); // initiol.. , comparator
                private int topK;

                public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                        StringTokenizer itr = new StringTokenizer(value.toString(), "::");
                        String category = itr.nextToken();
                        double rate = Double.parseDouble(itr.nextToken().trim());
                        insertTop(queue, category, rate, topK);
                }

                protected void setup(Context context) throws IOException, InterruptedException {
                        Configuration conf = context.getConfiguration();
                        topK = conf.getInt("topK", -1);
                        queue = new PriorityQueue<Youtube>(topK, comp);
                }

                protected void cleanup(Context context) throws IOException, InterruptedException {
                        while (queue.size() != 0) {
                                Youtube y = (Youtube)queue.remove();
                                context.write(new Text(y.getString()), NullWritable.get());
                        }
                }
        }

        public static class TopKReducer2 extends Reducer<Text, NullWritable, Text, Text> {
                private Comparator<Youtube> comp = new YoutubeComparator();
                private PriorityQueue<Youtube> queue = new PriorityQueue<Youtube>(10, comp) ;
                private int topK;

                public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
                        StringTokenizer itr = new StringTokenizer(key.toString(), "|");
                        String category = itr.nextToken();
                        double rate = Double.parseDouble(itr.nextToken().trim());
                        insertTop(queue, category, rate, topK);
                }

                protected void setup(Context context) throws IOException, InterruptedException {
                        Configuration conf = context.getConfiguration();
                        topK = conf.getInt("topK", -1);
                        queue = new PriorityQueue<Youtube>(topK, comp);
                }

                protected void cleanup(Context context) throws IOException, InterruptedException {
                        while (queue.size() != 0) {
                                Youtube y = (Youtube) queue.remove();
                                StringTokenizer itr2 = new StringTokenizer(y.getString(), "|");
                                context.write(new Text(itr2.nextToken()), new Text(itr2.nextToken()));
                        }
                }
        }
        
        public static void main(String[] args) throws Exception {
                String first_result = "/first_result";
                Configuration conf = new Configuration();
                String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
                if (otherArgs.length != 3)
                {
                        System.err.println("Usage: YouTubeStudent20190996 <in> <out>");
                        System.exit(2);
                }

                int topK = Integer.parseInt(otherArgs[2]);
                conf.setInt("topK", topK);

                Job job1 = new Job(conf, "YouTube1");

                job1.setJarByClass(YouTubeStudent20190996.class);
                job1.setMapperClass(YouTubeStudent20190996Mapper.class);
                job1.setReducerClass(YouTubeStudent20190996Reducer.class);

                job1.setOutputKeyClass(Text.class);
                job1.setOutputValueClass(DoubleWritable.class);

                FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
                FileOutputFormat.setOutputPath(job1, new Path(first_result));
                FileSystem.get(job1.getConfiguration()).delete(new Path(first_result), true);
                job1.waitForCompletion(true);


                Job job2 = new Job(conf, "YouTube2");

                job2.setJarByClass(YouTubeStudent20190996.class);
                job2.setMapperClass(TopKMapper2.class);
                job2.setReducerClass(TopKReducer2.class);

                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(NullWritable.class);

                FileInputFormat.addInputPath(job2, new Path(first_result));
                FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
                FileSystem.get(job2.getConfiguration()).delete(new Path(otherArgs[1]), true);

                job2.waitForCompletion(true);
        }
}
