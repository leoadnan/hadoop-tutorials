package com.test;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BookXYearFrequency {
    public static class BookXMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        
        private final IntWritable one = new IntWritable(1);
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String tmpStr = value.toString();
            String[] singleBookData = tmpStr.split("\";\"");
            context.write(new Text(singleBookData[3]), one);
        }
    }
    
    public static class BookXReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        
        public void reduce(Text key, Iterator<IntWritable> values, Context context) throws IOException, InterruptedException{
            
            int frequencyForYear = 0;
            while(values.hasNext()){
                frequencyForYear += values.next().get();
            }
            
            context.write(key, new IntWritable(frequencyForYear));
        }
    }
    
     public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://master:9000/");

        Job job = Job.getInstance(conf, "Book Frequency");

        job.setJarByClass(BookXYearFrequency.class);
        job.setMapperClass(BookXMapper.class);
        job.setReducerClass(BookXReducer.class);
        job.setCombinerClass(BookXReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("BX-Books.csv"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
