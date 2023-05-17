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

        public static class IMDBStudent20190996Mapper extends Mapper<Object, Text, Text, IntWritable> {
                private Text outputKey = new Text();
                private IntWritable value = new IntWritable(1);

                public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                        String[] str = value.toString().split("::");
                        String genreList = str[str.length() - 1];
                        StringTokenizer itr = new StringTokenizer(genreList,"|");

                        while (itr.hasMoreTokens()) {
                                outputKey.set(itr.nextToken());
                                context.write(outputKey, value);
                        }
                }
        }

        public static class IMDBStudent20190996Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
                private IntWritable val = new IntWritable();
                public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                        int count = 0;

                        for(IntWritable i : values) {
                                count += i.get();
                        }
                        val.set(count);
                        context.write(key, val);
                }
        }

        public static void main(String[] args) throws Exception {
                Configuration conf = new Configuration();
                String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
                if (otherArgs.length != 2) 
                {
                        System.err.println("Usage: IMDBStudent20190996 <in> <out>");
                        System.exit(2);
                }

                Job job = new Job(conf, "IMDBStudent20190996");

                job.setJarByClass(IMDBStudent20190996.class);
                job.setMapperClass(IMDBStudent20190996Mapper.class);
                job.setCombinerClass(IMDBStudent20190996Reducer.class)
                job.setReducerClass(IMDBStudent20190996Reducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);

                FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
                FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
                FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);
                System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}
