package ch08;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import util.JobBuilder;

public class MinimalMapReduceWithDefaults extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MinimalMapReduceWithDefaults(), args);
	    System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
		if (job == null) {
			return -1;
		}

		job.setInputFormatClass(TextInputFormat.class);
//		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setMapperClass(Mapper.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setPartitionerClass(HashPartitioner.class);

		job.setNumReduceTasks(1);
		job.setReducerClass(Reducer.class);

	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.setOutputFormatClass(TextOutputFormat.class);
//	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
//	    FileOutputFormat.setCompressOutput(job, true);
//	    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

	    return job.waitForCompletion(true) ? 0 : 1;
	}
}