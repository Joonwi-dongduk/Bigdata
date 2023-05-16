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

public class UBERMapper extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
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
