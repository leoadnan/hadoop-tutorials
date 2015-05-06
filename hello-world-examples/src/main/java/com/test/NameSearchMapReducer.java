package com.test;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NameSearchMapReducer {
    public static class NameMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            //System.out.println(key +"================="+value);
            context.write(key, new Text(line));
        }
    }
    
    public static class NameReducer extends Reducer<LongWritable, Text, LongWritable, Text>{
        
        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            String name = "Ace";
            boolean notFound = true;
            Iterator<Text> itr=values.iterator();
            while(itr.hasNext()){
                String val = itr.next().toString();
                if (val.equals(name)){
                    context.write(key, new Text(val));
                    notFound = false;
                    break;
                }
            }
            
            if(notFound){
                System.out.printf("[%s] not found in the given dataset.", name);
            }
        }
    }
    
    public static void main (String args[]) throws IOException, InterruptedException, ClassNotFoundException{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop-master:9000/");
//        conf.set("mapreduce.framework.name", "yarn");
//        conf.set("yarn.resourcemanager.address", "hadoop-master:8050");
//        conf.set("yarn.resourcemanager.scheduler.addresss", "hadoop-master:8030");
//        conf.set("yarn.resourcemanager.resource-tracker.address", "hadoop-master:8025");
        
        Job job = Job.getInstance(conf, "Search Name");
        
        job.setJarByClass(NameSearchMapReducer.class);
        job.setMapperClass(NameMapper.class);
        job.setReducerClass(NameReducer.class);
        
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path("NAMES.TXT"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
