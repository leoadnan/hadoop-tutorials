package ch03;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.TimestampsFilter;


public class TSFilterExample {
	public static void main(String[] args) throws IOException {
		
		// Get instance of Default Configuration
		Configuration conf = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(conf);
		
		// Get table instance
		Table table = connection.getTable(TableName.valueOf("sales"));

		List<Long> ts = new ArrayList<Long>();
		ts.add(new Long(1438035164949L));
		ts.add(new Long(1438035164949L));

		//filter output the column values for specified timestamps
		Filter filter = new TimestampsFilter(ts);

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
