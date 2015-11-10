package com.examples;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;

import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.hadoop.cql3.CqlInputFormat;
import org.apache.cassandra.hadoop.BulkOutputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cassawordcount.CassaWordCountMapper;
import cassawordcount.CassaWordCountReducer;

import com.datastax.driver.core.Row;

/**
 * This counts the occurrences of words in ColumnFamily
 *   cql3_worldcount ( id uuid,
 *                   line  text,
 *                   PRIMARY KEY (id))
 *
 * For each word, we output the total number of occurrences across all body texts.
 *
 * When outputting to Cassandra, we write the word counts to column family
 *  output_words ( word text,
 *                 count_num text,
 *                 PRIMARY KEY (word))
 * as a {word, count} to columns: word, count_num with a row key of "word sum"
 */

public class BulkOutputWordCount extends Configured implements Tool {
	private static final Logger logger = LoggerFactory.getLogger(BulkOutputWordCount.class);
	static final String INPUT_MAPPER_VAR = "input_mapper";
	static final String KEYSPACE = "cql3_worldcount";
	static final String COLUMN_FAMILY = "inputs";

	static final String OUTPUT_REDUCER_VAR = "output_reducer";
	static final String OUTPUT_COLUMN_FAMILY = "output_words";

	private static final String OUTPUT_PATH_PREFIX = "/tmp/word_count";
	private static final String PRIMARY_KEY = "row_key";

	public static void main(String[] args) throws Exception {
		// Let ToolRunner handle generic command-line options
		ToolRunner.run(new Configuration(), new BulkOutputWordCount(), args);
		System.exit(0);
	}

	public static class NativeTokenizerMapper extends Mapper<Long, Row, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private ByteBuffer sourceColumn;

		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
		}

		public void map(Long key, Row row, Context context) throws IOException, InterruptedException {
			String value = row.getString("line");
			logger.debug("read {}:{}={} from {}", key, "line", value, context.getInputSplit());
			StringTokenizer itr = new StringTokenizer(value);
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

//	public static class ReducerToCassandra extends Reducer<Text, IntWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {
//		private Map<String, ByteBuffer> keys;
//		private ByteBuffer key;
//
//		protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {
//			keys = new LinkedHashMap<String, ByteBuffer>();
//		}
//
//		public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//			int sum = 0;
//			for (IntWritable val : values)
//				sum += val.get();
//			keys.put("word", ByteBufferUtil.bytes(word.toString()));
//			context.write(keys, getBindVariables(word, sum));
//		}
//
//		private List<ByteBuffer> getBindVariables(Text word, int sum) {
//			List<ByteBuffer> variables = new ArrayList<ByteBuffer>();
//			variables.add(ByteBufferUtil.bytes(String.valueOf(sum)));
//			return variables;
//		}
//	}

	public static class ReducerToCassandra extends Reducer<Text, IntWritable, ByteBuffer, List<Mutation>> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			List<Mutation> columnsToAdd = new ArrayList<Mutation>();
			Integer wordCount = 0;
			for (IntWritable value : values) {
				wordCount += value.get();
			}
			
			
			Column wordcol = new Column(ByteBuffer.wrap("word".getBytes()));
			wordcol.setValue(ByteBuffer.wrap(key.toString().getBytes()));
			wordcol.setTimestamp(new Date().getTime());

			
			Column countCol = new Column(ByteBuffer.wrap("count_num".getBytes()));
			countCol.setValue(ByteBuffer.wrap(wordCount.toString().getBytes()));
			countCol.setTimestamp(new Date().getTime());

			ColumnOrSuperColumn wordCosc = new ColumnOrSuperColumn();
			wordCosc.setColumn(countCol);
			wordCosc.setColumn(wordcol);

			Mutation countMut = new Mutation();
			countMut.column_or_supercolumn = wordCosc;

			columnsToAdd.add(countMut);
			context.write(ByteBuffer.wrap(key.toString().getBytes()), columnsToAdd);
		}
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance();
		job.setJobName("Cassandra-Bulkloader");
		job.setJarByClass(BulkOutputWordCount.class);

		ConfigHelper.setOutputColumnFamily(job.getConfiguration(), KEYSPACE, OUTPUT_COLUMN_FAMILY);
		ConfigHelper.setOutputKeyspace(job.getConfiguration(), KEYSPACE);
		ConfigHelper.setOutputRpcPort(job.getConfiguration(), "9160");
		ConfigHelper.setOutputInitialAddress(job.getConfiguration(), "localhost");
		ConfigHelper.setOutputPartitioner(job.getConfiguration(), "Murmur3Partitioner");
		job.getConfiguration().set("mapreduce.output.bulkoutputformat.buffersize", "64");
		job.setOutputFormatClass(BulkOutputFormat.class);
		
		job.setReducerClass(ReducerToCassandra.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
//		job.setOutputKeyClass(Map.class);
//		job.setOutputValueClass(List.class);

//		job.getConfiguration().set(PRIMARY_KEY, "word,sum");
//		String query = "UPDATE " + KEYSPACE + "." + OUTPUT_COLUMN_FAMILY + " SET count_num = ? ";
//		CqlConfigHelper.setOutputCql(job.getConfiguration(), query);
		
		job.setMapperClass(NativeTokenizerMapper.class);
		job.setInputFormatClass(CqlInputFormat.class);
		CqlConfigHelper.setInputCql(job.getConfiguration(), "select * from " + COLUMN_FAMILY + " where token(id) > ? and token(id) <= ? allow filtering");

		ConfigHelper.setInputInitialAddress(job.getConfiguration(), "localhost");
		ConfigHelper.setInputColumnFamily(job.getConfiguration(), KEYSPACE, COLUMN_FAMILY);
		ConfigHelper.setInputPartitioner(job.getConfiguration(),"Murmur3Partitioner");
//
		CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration(), "3");
		job.waitForCompletion(true);
		return 0;
	}
}