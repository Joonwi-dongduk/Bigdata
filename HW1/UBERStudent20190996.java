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

        public static class UBERStudent20190996Mapper extends Mapper<Object, Text, Text, Text> {
                Text outputKey = new Text();
                Text outputValue = new Text();
                public static String[] dateList = {"SUN", "MON", "TUE", "WED", "THR", "FRI", "SAT"};
                public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

                        String[] options = value.toString().split(",");
                        String region = options[0];
                        Date date = new Date(options[1]);
                        String trips = options[3];
                        String vehicles = options[2];
                        outputKey.set(region + "," + dateList[date.getDay()]);
                        outputValue.set(trips + "," + vehicles);

                        context.write(outputKey, outputValue);
                }
        }


        public static class UBERStudent20190996Reducer extends Reducer<Text, Text, Text, Text> {
                Text reduceValue = new Text();
                int totalTrips = 0;
                int totalVehicles = 0;

                public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

                        for(Text value : values) {
                                String[] valList = value.toString().split(",");
                                totalTrips += Integer.parseInt(valList[0]);
                                totalVehicles += Integer.parseInt(valList[1]);
                        }
                        reduceValue.set(totalTrips + "," + totalVehicles);
                        context.write(key, reduceValue);
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
