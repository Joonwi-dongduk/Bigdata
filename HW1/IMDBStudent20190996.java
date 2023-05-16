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

public class IMDBStudent20190996 {

        public static class IMDBMapper extends Mapper<Object, Text, Text, IntWritable> {
                private Text outputKey = new Text();

                public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                        String genreList = value.toString().split("::")[1];
                        StringTokenizer itr = new StringTokenizer(genreList,"|");

                        while (itr.hasMoreTokens()) {
                                outputKey.set(itr.nextToken());
                                context.write(outputKey, new IntWritable(1));
                        }
                }
        }

        public static class IMDBReducer extends Reducer<Text, IntWritable, Text, Text> {
                public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                        int count = 0;

                        for(IntWritable i : values) {
                                count += i.get();
                        }
                        context.write(key, new Text(String.valueOf(count)) );
                }
        }

        public static void main(String[] args) throws Exception {
                Configuration conf = new Configuration();
                Job job = new Job(conf, "IMDB");

                job.setJarByClass(IMDBStudent20190996.class);
                job.setMapperClass(IMDBMapper.class);
                job.setReducerClass(IMDBReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                job.setInputFormatClass(KeyValueTextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);

                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                FileSystem.get(job.getConfiguration()).delete(new Path(args[1]), true);
                job.waitForCompletion(true);
        }
}

