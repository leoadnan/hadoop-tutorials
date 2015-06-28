package ch05;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import util.Utils;

public class Test extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Test(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		IntWritable writable = new IntWritable(163);
		byte[] bytes = Utils.serialize(writable);
		System.out.println(bytes.length);
		
		IntWritable newWritable = new IntWritable();
		Utils.deserialize(newWritable, bytes);
		System.out.println(newWritable.get());
		
		BytesWritable bw = new BytesWritable(new byte[]{3,5});
		bytes =Utils.serialize(bw);
		System.out.println(StringUtils.byteToHexString(bytes));
		
		System.out.println(System.getProperty("TEST"));
		return 0;
	}

}
