package ch03.get;

// Example application retrieving data from HBase using a Java 7 construct
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

public class GetTryWithResourcesExample {

	public static void main(String[] args) throws IOException {

		// 1-CreateConf Create the configuration.
		Configuration conf = HBaseConfiguration.create();

		HBaseHelper helper = HBaseHelper.getHelper(conf);
		if (!helper.existsTable("testtable")) {
			helper.createTable("testtable", "colfam1");
		}

		try (Connection connection = ConnectionFactory.createConnection(conf);
		// 2-NewTable Instantiate a new table reference in "try" block.
			Table table = connection.getTable(TableName.valueOf("testtable"));) {
			Get get = new Get(Bytes.toBytes("row1"));
			get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
			
			Result result = table.get(get);
			byte[] val = result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
			System.out.println("Value: " + Bytes.toString(val));
		} // 3-Close No explicit close needed, Java will handle AutoClosable's.
		helper.close();
	}
}
