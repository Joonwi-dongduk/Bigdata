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

public class UBERStudent20190996 {
        
        public static class UBERMapper extends Mapper<Object, Text, Text, Text> {
                public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                        Text outputKey = new Text();
                        Text outputValue = new Text();

                        String[] options = key.toString().split(",");
                        String region = options[0];
                        String date = options[1];
                        String trips = options[3];
                        String vehicles = options[2];

                        String[] splitDate = date.split("/");
                        int year = Integer.parseInt(splitDate[2]);
                        int month = Integer.parseInt(splitDate[0]);
                        int day = Integer.parseInt(splitDate[1]);

                        LocalDate localDate = LocalDate.of(year, month, day);

                        outputKey.set(region);
                        outputValue.set(localDate.getDayOfWeek().toString().substring(0, 3) + "," + trips + "," + vehicles);

                        context.write(outputKey, outputValue);
                }
        }

        public static class UBERReducer extends Reducer<Text, Text, Text, Text> {
                public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                        Text reduceKey = new Text();
                        String valueString;

                        for(Text value : values) {
                                reduceKey.set(key + "," + value.toString().substring(0,3));
                                context.write(reduceKey, new Text(value.toString().substring(3)));
                        }
                }
        }


        public static void main(String[] args) throws Exception {
                Configuration conf = new Configuration();
                Job job = new Job(conf, "UBER");

                job.setJarByClass(UBERStudent20190996.class);
                job.setMapperClass(UBERMapper.class);
                job.setReducerClass(UBERReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);

                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                FileSystem.get(job.getConfiguration()).delete(new Path(args[1]), true);
                job.waitForCompletion(true);
        }
}
                                                                                                 89,1         바닥



