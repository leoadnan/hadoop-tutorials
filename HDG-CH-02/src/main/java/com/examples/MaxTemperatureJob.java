package com.examples;

import java.io.IOException;

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

public class MaxTemperatureJob {

	public static class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private static final int MISSING = 9999;
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String year = line.substring(15,19);
			
			int airTemperature;
			if(line.charAt(87) == '+'){
				airTemperature = Integer.parseInt(line.substring(88, 92));
			}
			else {
				airTemperature = Integer.parseInt(line.substring(87,92));
			}
			
			String quality = line.substring(92,93);
			if(airTemperature != MISSING && quality.matches("[01459]")){
				context.write(new Text(year), new IntWritable(airTemperature));
			}
		}
	}
	
	public static class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int maxValue = Integer.MIN_VALUE;
			
			for(IntWritable value : values){
				maxValue = Math.max(maxValue, value.get());
			}
			
			context.write(key, new IntWritable(maxValue));
		}
		
		
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://master:9000/");
//      conf.set("yarn.resourcemanager.address", "aahmed-mac.local:8032");

        Job job = Job.getInstance(conf,"Max Temperature");
        
        job.setJarByClass(MaxTemperatureJob.class);
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setCombinerClass(MaxTemperatureReducer.class);
        
        FileInputFormat.addInputPath(job, new Path("sample.txt"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        
        
        System.exit(job.waitForCompletion(true)?0:1);

	}

}
