package cassawordcount2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassaWordCountReducer extends Reducer<Text, IntWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {
	private Map<String, ByteBuffer> keys;
	private ByteBuffer key;

	private static final Logger logger = LoggerFactory.getLogger(CassaWordCountReducer.class);
	
	protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {
		keys = new LinkedHashMap<String, ByteBuffer>();
	}

	public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		logger.debug("***************************"+word.toString()+"***********************"+values);
		
		int sum = 0;
		for (IntWritable val : values)
			sum += val.get();
		keys.put("word", ByteBufferUtil.bytes(word.toString()));
		context.write(keys, getBindVariables(word, sum));
		
	}

	private List<ByteBuffer> getBindVariables(Text word, int sum) {
		List<ByteBuffer> variables = new ArrayList<ByteBuffer>();
		variables.add(ByteBufferUtil.bytes(String.valueOf(sum)));
		return variables;
	}
}
//public class CassaWordCountReducer extends Reducer<Text, IntWritable, ByteBuffer, List<Mutation>> {
//
//	@Override
//	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//		List<Mutation> columnsToAdd = new ArrayList<Mutation>();
//		Integer wordCount = 0;
//		for (IntWritable value : values) {
//			wordCount += value.get();
//		}
//		Column countCol = new Column(ByteBuffer.wrap("w_count".getBytes()));
//		countCol.setValue(ByteBuffer.wrap(wordCount.toString().getBytes()));
//		countCol.setTimestamp(new Date().getTime());
//
//		ColumnOrSuperColumn wordCosc = new ColumnOrSuperColumn();
//		wordCosc.setColumn(countCol);
//
//		Mutation countMut = new Mutation();
//		countMut.column_or_supercolumn = wordCosc;
//
//		columnsToAdd.add(countMut);
//		context.write(ByteBuffer.wrap(key.toString().getBytes()), columnsToAdd);
//	}
//
//}
