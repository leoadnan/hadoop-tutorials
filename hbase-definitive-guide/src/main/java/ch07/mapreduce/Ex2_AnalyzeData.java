package ch07.mapreduce;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

// MapReduce job that reads the imported data and analyzes it.
	public class Ex2_AnalyzeData {

	private static final Log LOG = LogFactory.getLog(Ex2_AnalyzeData.class);

	public static final String NAME = "Ex2_AnalyzeData";

	public enum Counters {
		ROWS, COLS, ERROR, VALID
	}
	
	// 1-Mapper Extend the supplied TableMapper class, setting your own output key and value types.
	static class AnalyzeMapper extends TableMapper<Text, IntWritable> {

		private JSONParser parser = new JSONParser();
		private IntWritable ONE = new IntWritable(1);

		@Override
		public void map(ImmutableBytesWritable row, Result columns, Context context) throws IOException {
			context.getCounter(Counters.ROWS).increment(1);
			String value = null;
			try {
				for (Cell cell : columns.listCells()) {
					context.getCounter(Counters.COLS).increment(1);
					value = Bytes.toStringBinary(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
					JSONObject json = (JSONObject) parser.parse(value);
					
					// 2-Parse Parse the JSON data, extract the author and count the occurrence.
					String author = (String) json.get("author");

					if (context.getConfiguration().get("conf.debug") != null)
						System.out.println("Author: " + author);

					context.write(new Text(author), ONE);
					context.getCounter(Counters.VALID).increment(1);
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("Row: " + Bytes.toStringBinary(row.get()) + ", JSON: " + value);
				context.getCounter(Counters.ERROR).increment(1);
			}
		}
    
    /*
       {
         "updated": "Mon, 14 Sep 2009 17:09:02 +0000",
         "links": [{
           "href": "http://www.webdesigndev.com/",
           "type": "text/html",
           "rel": "alternate"
         }],
         "title": "Web Design Tutorials | Creating a Website | Learn Adobe
             Flash, Photoshop and Dreamweaver",
         "author": "outernationalist",
         "comments": "http://delicious.com/url/e104984ea5f37cf8ae70451a619c9ac0",
         "guidislink": false,
         "title_detail": {
           "base": "http://feeds.delicious.com/v2/rss/recent?min=1&count=100",
           "type": "text/plain",
           "language": null,
           "value": "Web Design Tutorials | Creating a Website | Learn Adobe
               Flash, Photoshop and Dreamweaver"
         },
         "link": "http://www.webdesigndev.com/",
         "source": {},
         "wfw_commentrss": "http://feeds.delicious.com/v2/rss/url/
             e104984ea5f37cf8ae70451a619c9ac0",
         "id": "http://delicious.com/url/
             e104984ea5f37cf8ae70451a619c9ac0#outernationalist"
       }
    */
    
  }

  
	// 3-Reducer Extend a Hadoop Reducer class, assigning the proper types.
	static class AnalyzeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable one : values)
				count++; // 4-Count Count the occurrences and emit sum.

			if (context.getConfiguration().get("conf.debug") != null)
				System.out.println("Author: " + key.toString() + ", Count: " + count);

			context.write(key, new IntWritable(count));
		}
	}

	private static CommandLine parseArgs(String[] args) throws ParseException {
		Options options = new Options();
		Option o = new Option("t", "table", true, "table to read from (must exist)");
		o.setArgName("table-name");
		o.setRequired(true);
		options.addOption(o);
		
		o = new Option("c", "column", true, "column to read data from (must exist)");
		o.setArgName("family:qualifier");
		options.addOption(o);
		
		o = new Option("o", "output", true, "the directory to write to");
		o.setArgName("path-in-HDFS");
		o.setRequired(true);
		options.addOption(o);
		
		options.addOption("d", "debug", false, "switch on DEBUG log level");
		
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (Exception e) {
			System.err.println("ERROR: " + e.getMessage() + "\n");
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(NAME + " ", options, true);
			System.exit(-1);
		}
		if (cmd.hasOption("d")) {
			Logger log = Logger.getLogger("mapreduce");
			log.setLevel(Level.DEBUG);
			System.out.println("DEBUG ON");
		}
		return cmd;
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		CommandLine cmd = parseArgs(otherArgs);
		// check debug flag and other options
		if (cmd.hasOption("d"))
			conf.set("conf.debug", "true");
		// get details
		
		String table = cmd.getOptionValue("t");
		String column = cmd.getOptionValue("c");
		String output = cmd.getOptionValue("o");

		// 5-Scan Create and configure a Scan instance.
		Scan scan = new Scan();
		if (column != null) {
			byte[][] colkey = KeyValue.parseColumn(Bytes.toBytes(column));
			if (colkey.length > 1) {
				scan.addColumn(colkey[0], colkey[1]);
			} else {
				scan.addFamily(colkey[0]);
			}
		}

		Job job = Job.getInstance(conf, "Analyze data in " + table);
		job.setJarByClass(Ex2_AnalyzeData.class);
		
		// 6-Util Set up the table mapper phase using the supplied utility.
		TableMapReduceUtil.initTableMapperJob(table, scan, AnalyzeMapper.class, Text.class, IntWritable.class, job);
		job.setReducerClass(AnalyzeReducer.class);
		
		// 7-Output Configure the reduce phase using the normal Hadoop syntax.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job, new Path(output));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
