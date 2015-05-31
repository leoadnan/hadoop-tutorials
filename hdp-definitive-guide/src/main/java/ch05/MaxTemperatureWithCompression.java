package ch05;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperatureWithCompression {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String inputPath="sample.txt";
		String outputPath ="output_"+System.currentTimeMillis();

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000/");
		conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
		conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, DefaultCodec.class, CompressionCodec.class);
		
		Job job = Job.getInstance(conf, "Max Temperature with compression");
	    job.setJarByClass(MaxTemperatureWithCompression.class);

	    FileInputFormat.addInputPath(job, new Path(inputPath));
	    FileOutputFormat.setOutputPath(job, new Path(outputPath));
	    
	    job.setMapperClass(ch02.MaxTemperatureMapper.class);
	    /*[*/job.setCombinerClass(ch02.MaxTemperatureReducer.class)/*]*/;
	    job.setReducerClass(ch02.MaxTemperatureReducer.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    FileOutputFormat.setCompressOutput(job, true);
	    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
