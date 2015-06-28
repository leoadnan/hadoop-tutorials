package ch05;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class AirlineTextWriter {
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		String input = args[0];
		String output = args[1];
		
		Configuration conf = new Configuration();
		LongWritable key = new LongWritable();
		Text value = new Text();
		SequenceFile.Writer writer = null;
		try {
			FileSystem fs = FileSystem.get(URI.create(output), conf);
			Path inputPath = new Path(input);
			Path outputPath = new Path(output);

			
			if(fs.exists(outputPath)){
				fs.delete(outputPath, true);
			}

			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(inputPath)));
			writer= SequenceFile.createWriter(fs, conf, outputPath, key.getClass(), value.getClass());

			String line;
			long no=1;
			while ((line = reader.readLine()) != null) {
				key.set(no++);
				value.set(line);
				writer.append(key, value);
			}
			
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally{
			IOUtils.closeStream(writer);
		}
	}
}
