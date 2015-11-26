package ch04.filters.compare;

// Example using the value based filter
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class Ex4_ValueFilterExample {

	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();

		HBaseHelper helper = HBaseHelper.getHelper(conf);
		helper.dropTable("testtable");
		helper.createTable("testtable", "colfam1", "colfam2");
		System.out.println("Adding rows to table...");
		helper.fillTable("testtable", 1, 10, 10, "colfam1", "colfam2");

		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("testtable"));
		// 1- Create filter, while specifying the comparison operator and comparator.
		Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
				new SubstringComparator(".4"));

		Scan scan = new Scan();
		// 2- Set filter for the scan.
		scan.setFilter(filter);
		ResultScanner scanner = table.getScanner(scan);
		System.out.println("Results of scan:");
		for (Result result : scanner) {
			for (Cell cell : result.rawCells()) {
				// 3- Print out value to check that filter works.
				System.out.println("Cell: "
						+ cell+ ", Value: "
						+ Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
			}
		}
		scanner.close();

		Get get = new Get(Bytes.toBytes("row-5"));
		// 4- Assign same filter to Get instance.
		get.setFilter(filter);
		Result result = table.get(get);
		System.out.println("Result of get: ");
		for (Cell cell : result.rawCells()) {
			System.out.println("Cell: "
					+ cell + ", Value: "
					+ Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
		}
	}
}
