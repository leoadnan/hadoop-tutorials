package ch04.filters.compare;

//Example using a filter to include only specific column families
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class Ex2_FamilyFilterExample {

	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();

		HBaseHelper helper = HBaseHelper.getHelper(conf);
		helper.dropTable("testtable");
		helper.createTable("testtable", "colfam1", "colfam2", "colfam3","colfam4");
		System.out.println("Adding rows to table...");
		helper.fillTable("testtable", 1, 10, 2, "colfam1", "colfam2","colfam3", "colfam4");

		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("testtable"));

		// 1-Filter Create filter, while specifying the comparison operator and comparator.
		Filter filter1 = new FamilyFilter(CompareFilter.CompareOp.LESS,
				new BinaryComparator(Bytes.toBytes("colfam3")));

		Scan scan = new Scan();
		scan.setFilter(filter1);
		// 2-Scan over table while applying the filter.
		ResultScanner scanner = table.getScanner(scan);
		System.out.println("Scanning table... ");
		for (Result result : scanner) {
			System.out.println(result);
		}
		scanner.close();

		Get get1 = new Get(Bytes.toBytes("row-5"));
		get1.setFilter(filter1);
		// 3-Get a row while applying the same filter.
		Result result1 = table.get(get1);
		System.out.println("Result of get(): " + result1);

		Filter filter2 = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes("colfam3")));
		// 4-Create a filter on one column family while trying to retrieve another.
		Get get2 = new Get(Bytes.toBytes("row-5"));
		get2.addFamily(Bytes.toBytes("colfam1"));
		get2.setFilter(filter2);
		// 5-Get the same row while applying the new filter, this will return "NONE".
		Result result2 = table.get(get2);
		System.out.println("Result of get(): " + result2);
	}
}
