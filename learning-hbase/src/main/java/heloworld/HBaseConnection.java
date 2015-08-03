package heloworld;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseConnection {

	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(config);
		Admin admin = connection.getAdmin();
		HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf("sales"));
		
		
		//Create column family
		if (admin.tableExists(tableDesc.getTableName())) {
//			admin.disableTable(table.getTableName());
//			admin.deleteTable(table.getTableName());
			System.out.println("table already exists!");
		}
		else {
			tableDesc.addFamily(new HColumnDescriptor("customers"));
			tableDesc.addFamily(new HColumnDescriptor("orders"));
			admin.createTable(tableDesc);
			System.out.println("Customers table created");
		}
		
		TableName[] tl= admin.listTableNames();
		for (TableName tableName : tl) {
			System.out.println(tableName.getNameAsString());
		}
	
		Table salesTable = connection.getTable(TableName.valueOf("sales"));

		//Insert Data
		Put put = new Put(Bytes.toBytes("row-1"));
		put.addColumn(Bytes.toBytes("customers"), Bytes.toBytes("customerId"),Bytes.toBytes("1"));
		put.addColumn(Bytes.toBytes("customers"), Bytes.toBytes("name"),Bytes.toBytes("aahmed"));
		put.addColumn(Bytes.toBytes("customers"), Bytes.toBytes("email"),Bytes.toBytes("leo_adnan@hotmail.com"));
		put.addColumn(Bytes.toBytes("customers"), Bytes.toBytes("phone"),Bytes.toBytes("1234567890"));
		put.addColumn(Bytes.toBytes("orders"), Bytes.toBytes("orderId"),Bytes.toBytes("1"));
		put.addColumn(Bytes.toBytes("orders"), Bytes.toBytes("product"),Bytes.toBytes("a"));

		salesTable.put(put);

		List<Put> puts = new ArrayList<Put>();
		for (int i=2; i<=100; i++){
			Put p = new Put(Bytes.toBytes("row-"+i));
			p.addColumn(Bytes.toBytes("customers"), Bytes.toBytes("customerId"),Bytes.toBytes(String.valueOf(i)));
			p.addColumn(Bytes.toBytes("customers"), Bytes.toBytes("name"),Bytes.toBytes("aahmed-"+i));
			p.addColumn(Bytes.toBytes("customers"), Bytes.toBytes("email"),Bytes.toBytes("leo_adnan@hotmail.com-"+i));
			p.addColumn(Bytes.toBytes("customers"), Bytes.toBytes("phone"),Bytes.toBytes("1234567890--"+i));
			p.addColumn(Bytes.toBytes("orders"), Bytes.toBytes("orderId"),Bytes.toBytes("1-"+i));
			p.addColumn(Bytes.toBytes("orders"), Bytes.toBytes("product"),Bytes.toBytes("a-"+i));
			puts.add(p);
		}
		salesTable.put(puts);
		
		//Get by row-id
		Get get = new Get(Bytes.toBytes("row-1"));
		Result rs = salesTable.get(get);
		Cell[] kv = rs.rawCells();
		for (Cell cell : kv) {
			System.out.print(new String(CellUtil.cloneRow(cell)) + " ");
			System.out.print(new String(CellUtil.cloneFamily(cell)) + ":");
			System.out.print(new String(CellUtil.cloneQualifier(cell)) + " ");
			System.out.print(cell.getTimestamp() + " ");
			System.out.println(new String(CellUtil.cloneValue(cell)));			
		}

		System.out.println("-------------------------------------------------");
		
		//Get by row-id range
		Scan s = new Scan();
		s.setStartRow(Bytes.toBytes("row-5"));
		s.setStopRow(Bytes.toBytes("row-9"));
		ResultScanner ss = salesTable.getScanner(s);
		for (Result r : ss) {
			kv = r.rawCells();
			for (Cell cell : kv) {
				System.out.print(new String(CellUtil.cloneRow(cell)) + " ");
				System.out.print(new String(CellUtil.cloneFamily(cell)) + ":");
				System.out.print(new String(CellUtil.cloneQualifier(cell)) + " ");
				System.out.print(new String(CellUtil.cloneValue(cell)) + " ");		
				System.out.println(cell.getTimestamp());
			}
			System.out.println("-------------------------------------------------");
		}
		ss.close();
		
//		List<Delete> list = new ArrayList<Delete>();
//		Delete del = new Delete("customer-2".getBytes());
//		list.add(del);
//		salesTable.delete(list);
//		salesTable.close();
	}

}
