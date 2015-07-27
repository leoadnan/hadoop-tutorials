package ch04.partitioner;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import ch04.LogWritable;

public class IPBasedPartitioner extends Partitioner<Text, LogWritable>{

	@Override
	public int getPartition(Text ipAddress, LogWritable value, int numPartitions) {
		StringTokenizer tokenizer  = new StringTokenizer(ipAddress.toString(), ".");
		if (tokenizer.hasMoreTokens()){
			String token = tokenizer.nextToken();			
			return ((token.hashCode() & Integer.MAX_VALUE) % numPartitions);
		}
		return 0;
	}
}
