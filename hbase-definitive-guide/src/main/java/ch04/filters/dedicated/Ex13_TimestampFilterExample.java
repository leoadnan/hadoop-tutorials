package ch04.filters.dedicated;

//Example filtering data by timestamps
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.TimestampsFilter;

import util.HBaseHelper;

public class Ex13_TimestampFilterExample {

	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();

		HBaseHelper helper = HBaseHelper.getHelper(conf);
		helper.dropTable("testtable");
		helper.createTable("testtable", "colfam1");
		System.out.println("Adding rows to table...");
		helper.fillTable("testtable", 1, 100, 20, true, "colfam1");

		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("testtable"));
		
		List<Long> ts = new ArrayList<Long>();
		ts.add(new Long(5));
		ts.add(new Long(10)); // 1- Add timestamps to the list.
		ts.add(new Long(15));
		Filter filter = new TimestampsFilter(ts);

		Scan scan1 = new Scan();
		scan1.setFilter(filter); // 2- Add the filter to an otherwise default Scan instance.
		ResultScanner scanner1 = table.getScanner(scan1);
		System.out.println("Results of scan #1:");
		for (Result result : scanner1) {
			System.out.println(result);
		}
		scanner1.close();

		Scan scan2 = new Scan();
		scan2.setFilter(filter);
		scan2.setTimeRange(8, 12); // 3- Also add a time range to verify how it affects the filter
		ResultScanner scanner2 = table.getScanner(scan2);
		System.out.println("Results of scan #2:");
		for (Result result : scanner2) {
			System.out.println(result);
		}
		scanner2.close();
	}
}
