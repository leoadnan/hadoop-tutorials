package ch05;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseCounterExample {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		// Get instance of Default Configuration
		Configuration conf = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(conf);
		
		// Get table instance
		Table table = connection.getTable(TableName.valueOf("mycounters"));

		table.incrementColumnValue(Bytes.toBytes("Jan14"), Bytes.toBytes("monthly"), Bytes.toBytes("hits"), 10L);
		
		table.close();
	}
}