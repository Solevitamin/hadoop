package de.th.wildau.hadoop;
import java.io.IOException;

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
 * @subject: Wie viele Fahrzeuge stehen am 31.12.2014 um 23:58 in Berlin zur Verfuegung? 
 */
public class JobOne {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                            
            try
            {
                String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
                   
                if(fileName.equals("2014-12-31-23-58-00-Berlin.json"))
                {
                    
                    JSONObject jsonObj = new JSONObject(value.toString());
                    JSONArray placemark = jsonObj.getJSONArray("placemarks");
                
                    for(int i=0; i < placemark.length();i++)
                    {
                        JSONObject place = (JSONObject) placemark.get(i);
                        word.set(place.getString("vin"));
                        context.write(word, one);
                    }
                }
            }catch (Exception e) { e.printStackTrace();}
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            
            result.set(sum);
            key.set("Anzahl freier Fahrzeuge in Berlin: ");
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
        job.setJarByClass(JobOne.class);
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

