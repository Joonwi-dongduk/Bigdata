import java.io.IOException;
import java.time.LocalDate;
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
        
        public static class UBERStudent20190996Mapper extends Mapper<Object, Text, Text, Text> {
                Text outputKey = new Text();
                Text outputValue = new Text();
                public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

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

        public static class UBERStudent20190996Reducer extends Reducer<Text, Text, Text, Text> {
                Text reduceKey = new Text();
                public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                        
                        for(Text value : values) {
                                reduceKey.set(key + "," + value.toString().substring(0,3));
                                context.write(reduceKey, new Text(value.toString().substring(3)));
                        }
                }
        }


        public static void main(String[] args) throws Exception {
                Configuration conf = new Configuration();
                String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
                if (otherArgs.length != 2) 
                {
                        System.err.println("Usage: UBERStudent20190996 <in> <out>");
                        System.exit(2);
                }

                Job job = new Job(conf, "UBER");

                job.setJarByClass(UBERStudent20190996.class);
                job.setMapperClass(UBERStudent20190996Mapper.class);
                job.setReducerClass(UBERStudent20190996Reducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
                FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
                FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);
                System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}
