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


/**
 *
 * @author Philipp
 * @subject: Wie oft wurde in diesem Zeitraum ein bestimmtes Auto bewegt?
 * 
 */
public class JobThree {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private String vin = "WME4513341K582087";

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                    
            String name = ((FileSplit) context.getInputSplit()).getPath().getName().replace(".json", "");
            char[] array = new char[name.length()];

            for (int i = 0; i < array.length; i++ ){
                if(i == 4 || i== 7){            array[i] = ".".charAt(0);
                }else if( i == 10){             array[i] = " ".charAt(0);
                }else if( i == 13){             array[i] = ":".charAt(0);
                }else if( i >= 16 && i<=19){    array[i] = " ".charAt(0);
                }else{                          array[i] = name.charAt(i); }
            }

            word.set(new String(array));
            if(value.toString().contains(vin)) one.set(1);
            else one.set(0);
            context.write(word, one);
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
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: usage of determined vihicle <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "vihicle count");
        job.setJarByClass(JobThree.class);
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

