package de.th.wildau.hadoop;
import com.google.gson.JsonArray;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONArray;


/**
 *
 * @author Philipp
 */
public class JobTwoExtended {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                            
            try
            {
                JSONObject jsonObj = new JSONObject(value.toString());
                JSONArray placemark = jsonObj.getJSONArray("placemarks");
                
                String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
                   
                if(fileName.contains("Berlin"))
                {
                    for(int i=0; i < placemark.length();i++)
                    {
                        word.set(fileName.replace("-Berlin.json", ""));
                        context.write(word, one);
                    }
                }
            }catch (Exception e) { e.printStackTrace();}
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();
        private int maxCount;
        private String maxTime;

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            
            if(sum > maxCount)
            {
                maxCount = sum;
                maxTime = key.toString();
            }
            
            
            result.set(maxCount);
            key.set(maxTime);
            
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: count vihicle in Berlin <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "vihicle count");
        job.setJarByClass(JobTwoExtended.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
