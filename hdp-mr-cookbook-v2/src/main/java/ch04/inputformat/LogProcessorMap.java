package ch04.inputformat;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ch04.LogWritable;

public class LogProcessorMap extends Mapper<LongWritable, LogWritable, Text, LogWritable> {

	public void map(LongWritable key, LogWritable value, Context context) throws IOException, InterruptedException {

		context.write(value.getUserIP(), value);
	}

}
