package ch03.put;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

public class PutExample {

	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();

		HBaseHelper helper = HBaseHelper.getHelper(conf);
		helper.dropTable("testtable");
		helper.createTable("testtable", "colfam1");

		Connection connection = ConnectionFactory.createConnection(conf);

		// NewTable Instantiate a new client.
		Table table = connection.getTable(TableName.valueOf("testtable"));

		// NewPut Create ch03.put with specific row.
		Put put = new Put(Bytes.toBytes("row1"));

		// Add a column, whose name is "colfam1:qual1", to the ch03.put.
		put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));

		// Add another column, whose name is "colfam1:qual2", to the ch03.put.
		put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("val2"));

		// Put Store row with column into the HBase table.
		table.put(put);

		// Put Close table and connection instances to free resources.
		table.close();
		connection.close();

		helper.close();
	}
}