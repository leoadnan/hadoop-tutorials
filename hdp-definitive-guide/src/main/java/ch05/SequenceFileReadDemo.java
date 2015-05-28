package ch05;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileReadDemo {

	public static void main(String[] args) {
		String inputFileName = args[0];

		Configuration conf = new Configuration();
		Path path = new Path(inputFileName);
		SequenceFile.Reader reader = null;	
		
		try {
			FileSystem fs = FileSystem.get(URI.create(inputFileName), conf);
			reader = new SequenceFile.Reader(fs, path, conf);
			
			IntWritable key = (IntWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Text value = (Text)ReflectionUtils.newInstance(reader.getValueClass(), conf);
			
			long position = reader.getPosition();
			while (reader.next(key, value)){
				String syncSeen = reader.syncSeen() ? "*" : "";
				System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
				position = reader.getPosition(); // beginning of next record
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(reader);
		}
	}

}
