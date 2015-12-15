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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

// MapReduce job that parses the raw data into separate tables.
public class Ex4_ParseJsonMulti {

	private static final Log LOG = LogFactory.getLog(Ex4_ParseJsonMulti.class);

	public static final String NAME = "Ex4_ParseJsonMulti";

	public enum Counters {
		ROWS, COLS, ERROR, VALID
	}

	static class ParseMapper extends
			TableMapper<ImmutableBytesWritable, Writable> {

		private Connection connection = null;
		private BufferedMutator infoTable = null;
		private BufferedMutator linkTable = null;
		private JSONParser parser = new JSONParser();
		private byte[] columnFamily = null;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			connection = ConnectionFactory.createConnection(context.getConfiguration());
			// 1-Setup Create and configure both target tables in the setup() method.
			infoTable = connection.getBufferedMutator(TableName.valueOf(context.getConfiguration().get("conf.infotable")));
			linkTable = connection.getBufferedMutator(TableName.valueOf(context.getConfiguration().get("conf.linktable")));
			columnFamily = Bytes.toBytes(context.getConfiguration().get("conf.columnfamily"));
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// 2-Cleanup Flush all pending commits when the task is complete.
			infoTable.flush();
			linkTable.flush();
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
		
		@Override
		public void map(ImmutableBytesWritable row, Result columns, Context context) throws IOException {
			context.getCounter(Counters.ROWS).increment(1);
			String value = null;
			try {
				
				Put infoPut = new Put(row.get());
				Put linkPut = new Put(row.get());
				
				for (Cell cell : columns.listCells()) {
					context.getCounter(Counters.COLS).increment(1);
					value = Bytes.toStringBinary(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
					JSONObject json = (JSONObject) parser.parse(value);
					for (Object key : json.keySet()) {
						Object val = json.get(key);
						if ("link".equals(key)) {
							linkPut.addColumn(columnFamily,Bytes.toBytes(key.toString()),Bytes.toBytes(val.toString()));
						} else {
							infoPut.addColumn(columnFamily,Bytes.toBytes(key.toString()),Bytes.toBytes(val.toString()));
						}
					}
				}
				// 3-TwoTbls Save parsed values into two separate tables.
				infoTable.mutate(infoPut);
				linkTable.mutate(linkPut);
				context.getCounter(Counters.VALID).increment(1);
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("Error: " + e.getMessage() + ", Row: " + Bytes.toStringBinary(row.get()) + ", JSON: " + value);
				context.getCounter(Counters.ERROR).increment(1);
			}
		}

	}

	private static CommandLine parseArgs(String[] args) throws ParseException {
		Options options = new Options();
		Option o = new Option("i", "input", true, "table to read from (must exist)");
		o.setArgName("input-table-name");
		o.setRequired(true);
		options.addOption(o);
		
		o = new Option("c", "column", true, "column to read data from (must exist)");
		o.setArgName("family:qualifier");
		options.addOption(o);
		
		o = new Option("o", "infotbl", true,"info table to write to (must exist)");
		o.setArgName("info-table-name");
		o.setRequired(true);
		options.addOption(o);
		
		o = new Option("l", "linktbl", true, "link table to write to (must exist)");
		o.setArgName("link-table-name");
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
		String input = cmd.getOptionValue("i");
		String column = cmd.getOptionValue("c");

		// 4-Conf Store table names in configuration for later use in the mapper.
		conf.set("conf.infotable", cmd.getOptionValue("o"));
		conf.set("conf.linktable", cmd.getOptionValue("l"));

		Scan scan = new Scan();
		if (column != null) {
			byte[][] colkey = KeyValue.parseColumn(Bytes.toBytes(column));
			if (colkey.length > 1) {
				scan.addColumn(colkey[0], colkey[1]);
				conf.set("conf.columnfamily", Bytes.toStringBinary(colkey[0]));
				conf.set("conf.columnqualifier", Bytes.toStringBinary(colkey[1]));
			} else {
				scan.addFamily(colkey[0]);
				conf.set("conf.columnfamily", Bytes.toStringBinary(colkey[0]));
			}
		}

		Job job = Job.getInstance(conf, "Parse data in " + input + ", into two tables");
		job.setJarByClass(Ex4_ParseJsonMulti.class);
		TableMapReduceUtil.initTableMapperJob(input, scan, ParseMapper.class, ImmutableBytesWritable.class, Put.class, job);

		// 5-Null Set the output format to be ignored by the framework.
		job.setOutputFormatClass(NullOutputFormat.class);
		job.setNumReduceTasks(0);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
