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
                        Calendar cal = Calendar.getInstance();
                        Text outputKey = new Text();
                        Text outputValue = new Text();

                        String key_string = key.toString();
                        String region = key_string.split(",")[0];
                        String date = key_string.split(",")[1];
                        String trips = key_string.split(",")[3];
                        String vehicles = key_string.split(",")[2];

                        int year = Integer.parseInt(date.split("/")[2]);
                        int month = Integer.parseInt(date.split("/")[0]);
                        int day = Integer.parseInt(date.split("/")[1]);

                        cal.set(year, month - 1, day);

                        outputKey.set(region);
                        outputValue.set(Integer.toString(cal.DAY_OF_WEEK) + "," + trips + "," + vehicles);

                        context.write(outputKey, outputValue);
                }
        }


        public static class UBERReducer extends Reducer<Text, Text, Text, Text> {
                public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                        Text reduceKey = new Text();
                        String valueString;

                        for(Text value : values) {
                                int date = Integer.valueOf(value.toString().split(",")[0]);
                                switch(date) {
                                        case 1: reduceKey.set(key + ",MON");
                                                break;
                                        case 2: reduceKey.set(key + ",TUE");
                                                break;
                                        case 3: reduceKey.set(key + ",WED");
                                                break;
                                        case 4: reduceKey.set(key + ",THR");
                                                break;
                                        case 5: reduceKey.set(key + ",FRI");
                                                break;
                                        case 6: reduceKey.set(key + ",SAT");
                                                break;
                                        case 7: reduceKey.set(key + ",SUN");
                                                break;
                                }

                                valueString = value.toString().split(",")[1] + "," + value.toString().split(",")[2];
                                context.write(reduceKey, new Text(valueString));
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



