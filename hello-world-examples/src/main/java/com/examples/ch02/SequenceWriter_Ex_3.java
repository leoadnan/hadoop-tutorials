package com.examples.ch02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SequenceWriter_Ex_3 extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Path inputPath = new Path("/user/aahmed/weblog_entries.txt");
        Path outputPath = new Path("/user/aahmed/weblog_entries2.txt");

        Configuration conf = getConf();
        //conf.set("fs.defaultFS", "hdfs://master:9000/");
        Job weblogJob = Job.getInstance(conf, "Sequence File Writer");

        weblogJob.setJarByClass(getClass());
        weblogJob.setNumReduceTasks(0);

        weblogJob.setMapperClass(Mapper.class);
        weblogJob.setMapOutputKeyClass(LongWritable.class);
        weblogJob.setMapOutputValueClass(Text.class);

        weblogJob.setOutputKeyClass(LongWritable.class);
        weblogJob.setOutputValueClass(Text.class);

        weblogJob.setInputFormatClass(TextInputFormat.class);
        weblogJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(weblogJob, inputPath);
        SequenceFileOutputFormat.setOutputPath(weblogJob, outputPath);
        
        //SequenceFileOutputFormat.setCompressOutput(weblogJob, true);
        //SequenceFileOutputFormat.setOutputCompressionType(weblogJob,SequenceFile.CompressionType.BLOCK);
        //SequenceFileOutputFormat.setOutputCompressorClass(weblogJob,GzipCodec.class);

        if (weblogJob.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }

    public static void main(String[] args) throws Exception {
        int returnCode = ToolRunner.run(new SequenceWriter_Ex_3(), args);
        System.exit(returnCode);
    }
}
