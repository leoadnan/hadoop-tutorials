package ch03.put;

// Example application inserting data into HBase
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

public class Ex3_PutExample {

	public static void main(String[] args) throws IOException {

		// 1-CreateConf Create the required configuration.
		Configuration conf = HBaseConfiguration.create();

		HBaseHelper helper = HBaseHelper.getHelper(conf);
		helper.dropTable("testtable");
		helper.createTable("testtable", "colfam1");

		Connection connection = ConnectionFactory.createConnection(conf);

		// 2-NewTable Instantiate a new client.
		Table table = connection.getTable(TableName.valueOf("testtable"));

		// 3-NewPut Create put with specific row.
		Put put = new Put(Bytes.toBytes("row1"));

		// 4-AddCol1 Add a column, whose name is "colfam1:qual1", to the put.
		put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
		// 4-AddCol2 Add another column, whose name is "colfam1:qual2", to the put.
		put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("val2"));

		// 5-DoPut Store row with column into the HBase table.
		table.put(put);
		
		// 6-DoPut Close table and connection instances to free resources.
		table.close();
		connection.close();
		helper.close();
	}
}
