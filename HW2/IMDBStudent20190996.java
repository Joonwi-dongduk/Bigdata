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

        public static class Movie {
                public String title;
                public double rate;

                public Movie (String title, double rate) {
                        this.title = title;
                        this.rate = rate;
                }
                public String getString() {
                        return title + "|" + rate;
                }
        }

        public static class MovieComparator implements Comparator<Movie> {
                public int compare(Movie m1, Movie m2) {
                        if (m1.rate > m2.rate)
                                return 1;
                        else if (m1.rate < m2.rate)
                                return -1;
                        return 0;
                }
        }

        public static void insertTop(PriorityQueue q, String title, double rate, int topK) {
                Movie movie = (Movie)q.peek();
                if (q.size() < topK || movie.rate < rate) {
                        Movie m = new Movie(title, rate);
                        q.add(m);
                        if (q.size() > topK)
                                q.remove();
                }
        }
        
        public static class IMDBStudent20190996Mapper extends Mapper<Object, Text, Text, Text> {

                boolean fileA = true;

                public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                        String joinKey;
                        String o_value;
                        String[] str = value.toString().split("::");

                        if (fileA) {
                                //[1]: joinKey, A[2]: o_value
                                joinKey = str[1];
                                o_value = "A," + str[2];
                        } else {
                                //[0]: joinKey, B[1]: o_value
                                joinKey = str[0];
                                o_value = "B," + str[1];
                        }
                        context.write(new Text(joinKey), new Text(o_value));
                }
                protected void setUp(Context context) throws IOException, InterruptedException {
                        String filename = ((FileSplit) context.getInputSplit()).getPath().getName();

                        if (filename.indexOf("ratings.dat") != -1 ) fileA = true;
                        else fileA = false;
                }
        }

        public static class IMDBStudent20190996Reducer extends Reducer<Text, Text, Text, Text> {

                public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

                        Text reduceKey = new Text();
                        Text reduceValue = new Text();
                        ArrayList<String> buffer = new ArrayList<String>();
                        int total = 0;
                        double avgRate;

                        for (Text val : values) {
                                //val[0] = A: file_type = "A", else = "B"
                                String file_type = val.toString().split(",")[0];
                                String str = val.toString().split(",")[1];
                                if (file_type.equals("B")) {
                                        reduceKey.set(str);// 영화이름, val 뒷부분, reduceKey 설정
                                } else {
                                        buffer.add(str);
                                }
                        }
                        for (String v : buffer) {
                                //total 변수에 별점 추가
                                total += Integer.parseInt(v);
                        }
                        //별점 buffer전체크기로 나누기, reduceValue 설정
                        avgRate = (double)total / (double)buffer.size();
                        reduceValue.set(String.valueOf(avgRate));

                        context.write(reduceKey, reduceValue);
                }
        }



        public static class TopKMapper2 extends Mapper<Object, Text, Text, NullWritable> {
                private Comparator<Movie> comp = new MovieComparator();
                private PriorityQueue<Movie> queue = new PriorityQueue<Movie>(10, comp); // initiol.. , comparator
                private int topK;

                public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                        StringTokenizer itr = new StringTokenizer(value.toString(), "::");
                        String title = itr.nextToken();
                        double rate = Double.parseDouble(itr.nextToken().trim());
                        insertTop(queue, title, rate, topK);
                }

                protected void setup(Context context) throws IOException, InterruptedException {
                        Configuration conf = context.getConfiguration();
                        topK = conf.getInt("topK", -1);
                        queue = new PriorityQueue<Movie>(topK, comp);
                }

                protected void cleanup(Context context) throws IOException, InterruptedException {
                        while (queue.size() != 0) {
                                Movie m = (Movie)queue.remove();
                                context.write(new Text(m.getString()), NullWritable.get());
                        }
                }
        }
        
        public static class TopKReducer2 extends Reducer<Text, NullWritable, Text, Text> {
                private Comparator<Movie> comp = new MovieComparator();
                private PriorityQueue<Movie> queue = new PriorityQueue<Movie>(10, comp) ;
                private int topK;

                public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
                        StringTokenizer itr = new StringTokenizer(key.toString(), "|");
                        String title = itr.nextToken();
                        double rate = Double.parseDouble(itr.nextToken().trim());
                        insertTop(queue, title, rate, topK);
                }

                protected void setup(Context context) throws IOException, InterruptedException {
                        Configuration conf = context.getConfiguration();
                        topK = conf.getInt("topK", -1);
                        queue = new PriorityQueue<Movie>(topK, comp);
                }

                protected void cleanup(Context context) throws IOException, InterruptedException {
                        while (queue.size() != 0) {
                                Movie m = (Movie) queue.remove();
                                StringTokenizer itr2 = new StringTokenizer(m.getString(), "|");
                                context.write(new Text(itr2.nextToken()), new Text(itr2.nextToken()));
                        }
                }
        }
        
        public static void main(String[] args) throws Exception {
                String first_result = "/first_rslt";
                Configuration conf = new Configuration();
                String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
                if (otherArgs.length != 3)
                {
                        System.err.println("Usage: IMDBStudent20190996 <in> <out> <k>");
                        System.exit(2);
                }
                int topK = Integer.parseInt(otherArgs[2]);
                conf.setInt("topK", topK);

                Job job1 = new Job(conf, "IMDBStudent20190996");

                job1.setJarByClass(IMDBStudent20190996.class);
                job1.setMapperClass(IMDBStudent20190996Mapper.class);
                job1.setReducerClass(IMDBStudent20190996Reducer.class);

                job1.setOutputKeyClass(Text.class);
                job1.setOutputValueClass(Text.class);

                FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
                FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
                FileSystem.get(job1.getConfiguration()).delete(new Path(otherArgs[1]), true);
                job1.waitForCompletion(true);


                Job job2 = new Job(conf, "IMDB2");

                job2.setJarByClass(IMDBStudent20190996.class);
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
