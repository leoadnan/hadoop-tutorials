package ch03;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class ScanExample {
	public static void main(String[] args) throws IOException {
		// Get instance of Default Configuration
		Configuration conf = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(conf);
		
		// Get table instance
		Table table = connection.getTable(TableName.valueOf("sales"));

		// Create Scan instance
		Scan scan = new Scan();

		// Add a column with value "Hello", in ìcf1:greetî, to the Put.
		scan.addColumn(Bytes.toBytes("customers"), Bytes.toBytes("email"));

		// Set Start Row
		scan.setStartRow(Bytes.toBytes("customer-1"));

		// Set End Row
		scan.setStopRow(Bytes.toBytes("customer-1437009409843"));

		// Get Scanner Results
		ResultScanner scanner = table.getScanner(scan);

		for (Result res : scanner) {
			System.out.println("Row Value: " + res);
		}
		scanner.close();
		table.close();
	}
}
