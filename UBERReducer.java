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

public class UBERReducer extends Reducer<Text, Text, Text, Text> {
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
