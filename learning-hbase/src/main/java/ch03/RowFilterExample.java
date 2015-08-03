package ch03;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class RowFilterExample {
	public static void main(String[] args) throws IOException {

		// Get instance of Default Configuration
		Configuration conf = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(conf);
		
		// Get table instance
		Table table = connection.getTable(TableName.valueOf("sales"));

		// Create Scan instance
		Scan scan = new Scan();

		// Add a column with value "Hello", in �cf1:greet�, to the Put.
		scan.addColumn(Bytes.toBytes("customers"), Bytes.toBytes("customerId"));

		// Filter using the regular expression
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("row-*"));
		
		scan.setFilter(filter);
				
		// Get Scanner Results
		ResultScanner scanner = table.getScanner(scan);

		for (Result res : scanner) {
			System.out.println("Row Value: " + res);
		}
		scanner.close();
		table.close();
	}
}
