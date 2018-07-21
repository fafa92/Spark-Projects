
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
public class FirsName_LastName_Avg {

    public static class JobMapper extends Mapper<LongWritable, Text, Text, Text> {

        Set<String> tokens;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            tokens = new HashSet<>();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (!value.toString().startsWith("id,name")) {
                String[] fields = value.toString().split(",");
                if ((fields.length > 3)) {
                    String event = fields[3];
                    String page_count = fields[18];
                    System.out.println(event);
                    event = event.replaceAll("-", "");
                    event = event.replaceAll("'", "");
                    event = event.replaceAll("(?:--|[\\\\[\\\\]{}()+/\\\\\\\\?!@#^ˆ;.&])", " ");
                    event = event.replaceAll("\\[", " ");
                    event = event.replaceAll("\\]", " ");
                    event = event.replaceAll("\"", " ");
                    event = event.trim().replaceAll(" +", " ");
                    event = event.toLowerCase();
                    List<String> listb = new ArrayList<String>();
                    for (String s : event.split(" ")) {
                        listb.add(s);
                    }
                    Collections.sort(listb);
                    String result = String.join(" ", listb);
                    if (tokens.contains(result)) {
                        context.write(new Text(result), new Text(page_count.trim()));
                    } else {
                        context.write(new Text(result), new Text(event + "#" + page_count.trim()));
                    }

                }
            }
        }
    }

    public static class JobReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int total = 0;
            String fkey = null;
            for (Text val : values) {
                total++;
                if (val.toString().split("#").length == 2) {
                    fkey = val.toString().split("#")[0];
                    sum += Double.valueOf(val.toString().split("#")[1]);
                } else {
                    sum += Double.valueOf(val.toString());
                }

            }
            double average = sum / total;
            if ((!fkey.isEmpty()) && (!fkey.equals("\t"))) {
                context.write(new Text(fkey + "	" + total), new DoubleWritable(average));
            }

        }

    }

    public static class SortMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tmp = value.toString().split("\t");
            context.write(new Text(tmp[0]), new Text(tmp[1] + "\t" + tmp[2]));
        }

    }

    public static class SortReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }

    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: <in> <out> directories required!");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        Job job = new Job(conf, "FirsName_LastName_Avg App");
        job.setJarByClass(FirsName_LastName_Avg.class);
        job.setMapperClass(JobMapper.class);
        job.setReducerClass(JobReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "-tmp"));
        int res = job.waitForCompletion(true) ? 0 : 1;
        if (res == 0) {
            Configuration conf1 = new Configuration();
            Job job1 = new Job(conf1, "Sort_FirsName_LastName_Avg App");
            job1.setJarByClass(FirsName_LastName_Avg.class);
            job1.setMapperClass(SortMapper.class);
            job1.setReducerClass(SortReducer.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job1, new Path(args[1] + "-tmp"));
            FileOutputFormat.setOutputPath(job1, new Path(args[1]));
            System.exit(job1.waitForCompletion(true) ? 0 : 1);
        }
    }
}
