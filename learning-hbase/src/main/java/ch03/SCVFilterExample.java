package ch03;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class SCVFilterExample {
	public static void main(String[] args) throws IOException {
		
		// Get instance of Default Configuration
		Configuration conf = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(conf);
		
		// Get table instance
		Table table = connection.getTable(TableName.valueOf("sales"));
		
		SingleColumnValueFilter filter = new SingleColumnValueFilter(
				Bytes.toBytes("customers"), 
				Bytes.toBytes("customerId"),
				CompareFilter.CompareOp.EQUAL, 
				new SubstringComparator("1"));

		// By Default it is false.
		// If set as true, this restricts the rows
		// if the specified column is not present
		filter.setFilterIfMissing(true);

		// Create Scan instance
		Scan scan = new Scan();
		scan.setFilter(filter);

		// Get Scanner Results
		ResultScanner scanner = table.getScanner(scan);

		for (Result res : scanner) {
//			System.out.println("Row Value: " + res);
			Cell[] kv = res.rawCells();
			for (Cell cell : kv) {
				System.out.print(new String(CellUtil.cloneRow(cell)) + " ");
				System.out.print(new String(CellUtil.cloneFamily(cell)) + ":");
				System.out.print(new String(CellUtil.cloneQualifier(cell)) + " ");
				System.out.print(new String(CellUtil.cloneValue(cell)) + " ");		
				System.out.println(cell.getTimestamp());
			}
			System.out.println("-------------------------------------------------");
		}
		scanner.close();
		table.close();
	}
}
