/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.examples.ch02;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author aahmed
 */
public class HdfsReader_Ex_2 extends Configured implements Tool{
    public int run(String[] args) throws Exception {
           Path inputPath = new Path(args[0]);
           String localOutputPath = args[1];
           Configuration conf = getConf();
           FileSystem fs = FileSystem.get(conf);
           InputStream is = fs.open(inputPath);
           OutputStream os = new BufferedOutputStream(
               new FileOutputStream(localOutputPath));
           IOUtils.copyBytes(is, os, conf);
           return 0;
}
       public static void main(String[] args) throws Exception {
           int returnCode = ToolRunner.run(new HdfsReader_Ex_2(), args);
        System.exit(returnCode);
}
}
