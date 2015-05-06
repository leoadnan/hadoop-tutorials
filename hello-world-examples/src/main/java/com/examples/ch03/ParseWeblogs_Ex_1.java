/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.examples.ch03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author aahmed
 */
public class ParseWeblogs_Ex_1 extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Path inputPath = new Path("apache_clf.txt");
        Path outputPath = new Path("output");
        Configuration conf = getConf();
        Job weblogJob = Job.getInstance(conf);
        weblogJob.setJobName("Weblog Transformer");
        weblogJob.setJarByClass(getClass());
        weblogJob.setNumReduceTasks(0);
        
        weblogJob.setMapperClass(CLFMapper_Ex_1.class);
        weblogJob.setMapOutputKeyClass(Text.class);
        weblogJob.setMapOutputValueClass(Text.class);
        
        weblogJob.setOutputKeyClass(Text.class);
        weblogJob.setOutputValueClass(Text.class);
        
        weblogJob.setInputFormatClass(TextInputFormat.class);
        weblogJob.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(weblogJob, inputPath);
        FileOutputFormat.setOutputPath(weblogJob, outputPath);
        
        if (weblogJob.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }

    public static void main(String[] args) throws Exception {
        int returnCode = ToolRunner.run(new ParseWeblogs_Ex_1(), args);
        System.exit(returnCode);
    }

}
