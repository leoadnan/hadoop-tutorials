package com.examples.ch02;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvroWriter_Ex_5 extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Path inputPath = new Path("weblog_entries.txt");
        Path outputPath = new Path("output");
        Schema schema = ReflectData.get().getSchema(WeblogRecord.class);
        Configuration conf = getConf();
        
        
        
        Job weblogJob = Job.getInstance(conf);
        weblogJob.setJobName("Avro Writer");
        weblogJob.setJarByClass(getClass());
        
        weblogJob.setNumReduceTasks(0);
        weblogJob.setMapperClass(WeblogMapper_Ex_5.class);
        weblogJob.setMapOutputKeyClass(AvroWrapper.class);
        weblogJob.setMapOutputValueClass(NullWritable.class);
        
        weblogJob.setInputFormatClass(TextInputFormat.class);
        
        AvroJob.setOutputKeySchema(weblogJob, schema);
        
        FileInputFormat.setInputPaths(weblogJob, inputPath);
        FileOutputFormat.setOutputPath(weblogJob, outputPath);
        
        if (weblogJob.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }

    public static void main(String[] args) throws Exception {
        int returnCode = ToolRunner.run(new AvroWriter_Ex_5(), args);
        System.exit(returnCode);
    }
}
