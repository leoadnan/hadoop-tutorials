package ch04.filters.compare;

//Example using a filter to select specific rows
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
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class Ex1_RowFilterExample {

	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();

		HBaseHelper helper = HBaseHelper.getHelper(conf);
		helper.dropTable("testtable");
		helper.createTable("testtable", "colfam1", "colfam2");
		System.out.println("Adding rows to table...");
		helper.fillTable("testtable", 1, 100, 100, 1, false, "colfam1", "colfam2");

		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("testtable"));
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-1"));

		// 1- Create filter, while specifying the comparison operator and comparator. Here an exact match is needed.
		Filter filter1 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("row-022")));
		scan.setFilter(filter1);
		ResultScanner scanner1 = table.getScanner(scan);
		System.out.println("Scanning table #1...");
		for (Result res : scanner1) {
			System.out.println(res);
		}
		scanner1.close();

		// 2- Another filter, this time using a regular expression to match the row keys.
		Filter filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(".*-.5"));
		scan.setFilter(filter2);
		ResultScanner scanner2 = table.getScanner(scan);
		System.out.println("Scanning table #2...");
		for (Result res : scanner2) {
			System.out.println(res);
		}
		scanner2.close();

		// 3- The third filter uses a substring match approach.
		Filter filter3 = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("-5"));
		scan.setFilter(filter3);
		ResultScanner scanner3 = table.getScanner(scan);
		System.out.println("Scanning table #3...");
		for (Result res : scanner3) {
			System.out.println(res);
		}
		scanner3.close();
	}
}
