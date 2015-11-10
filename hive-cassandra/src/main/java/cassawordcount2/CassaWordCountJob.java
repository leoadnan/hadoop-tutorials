package cassawordcount2;

import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.hadoop.BulkOutputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlBulkOutputFormat;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.examples2.WordCount.ReducerToCassandra;

public class CassaWordCountJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CassaWordCountJob(), args);
        System.exit(exitCode); 
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.printf("Usage: %s [generic options] <input dir>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        Job job = Job.getInstance();
        job.setJobName(getClass().getName());
        job.setJarByClass(CassaWordCountJob.class);
        
//        Configuration conf = job.getConfiguration();
        
        // cassandra bulk loader config
//        conf.set("cassandra.output.keyspace", "cassa_word_count");
//        conf.set("cassandra.output.columnfamily", "words");
        // OrderPreservingPartitioner for example could be used here too
//        conf.set("cassandra.output.partitioner.class", "org.apache.cassandra.dht.RandomPartitioner");
//        conf.set("cassandra.output.thrift.port","9160");    // default
//        conf.set("cassandra.output.thrift.address", "127.0.0.1");
//        conf.set("mapreduce.output.bulkoutputformat.streamthrottlembits", "400");

        
		ConfigHelper.setOutputColumnFamily(job.getConfiguration(), "cassa_word_count", "words");
		ConfigHelper.setOutputKeyspace(job.getConfiguration(), "cassa_word_count");
		ConfigHelper.setOutputRpcPort(job.getConfiguration(), "9160");
		ConfigHelper.setOutputInitialAddress(job.getConfiguration(), "localhost");
		ConfigHelper.setOutputPartitioner(job.getConfiguration(), "Murmur3Partitioner");
		Config.setClientMode(true);
		job.getConfiguration().set("mapreduce.output.bulkoutputformat.localdir", "/Users/aahme25/sstable_data/");
		
//		job.getConfiguration().set("mapreduce.output.bulkoutputformat.buffersize", "64");
		
		
        job.setMapperClass(CassaWordCountMapper.class);
        job.setReducerClass(CassaWordCountReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Map.class);
		job.setOutputValueClass(List.class);

//		job.getConfiguration().set("row_key", "word,sum");
		String query = " insert into cassa_word_count.words (word,w_count) values ('1',?) ";
//		String query = " UPDATE cassa_word_count.words set w_count=? ";
//		CqlConfigHelper.setOutputCql(job.getConfiguration(), query);
		
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        
        job.setOutputFormatClass(CqlBulkOutputFormat.class);
//        CqlBulkOutputFormat.setTableInsertStatement(job.getConfiguration(), "words", query);
//        CqlBulkOutputFormat.setTableSchema(job.getConfiguration(), "words", "CREATE TABLE cassa_word_count.words (word text,w_count text,PRIMARY KEY (word))");
        CqlBulkOutputFormat.setColumnFamilySchema(job.getConfiguration(), "words", "CREATE TABLE cassa_word_count.words (word text,w_count text,PRIMARY KEY (word))");
        CqlBulkOutputFormat.setColumnFamilyInsertStatement(job.getConfiguration(), "words", query);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
