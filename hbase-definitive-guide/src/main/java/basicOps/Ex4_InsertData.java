package basicOps;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class Ex4_InsertData {
	public static void main(String[] args) throws IOException {

		// Instantiating configuration class
		Configuration config = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(config);

		// Get HbaseAdmin class
		Admin admin = connection.getAdmin();

		// Instantiating HTable class
		Table table = connection.getTable(TableName.valueOf("emp"));

		// Instantiating Put class
		// accepts a row name.
		Put p = new Put(Bytes.toBytes("row1"));

		// adding values using add() method
		// accepts column family name, qualifier/row name ,value
		p.addColumn(Bytes.toBytes("personal data"), Bytes.toBytes("name"),Bytes.toBytes("raju"));

		p.addColumn(Bytes.toBytes("personal data"), Bytes.toBytes("city"),Bytes.toBytes("hyderabad"));

		p.addColumn(Bytes.toBytes("professional data"), Bytes.toBytes("designation"),Bytes.toBytes("manager"));

		p.addColumn(Bytes.toBytes("professional data"), Bytes.toBytes("salary"),Bytes.toBytes("50000"));

		// Saving the put Instance to the HTable.
		table.put(p);
		System.out.println("data inserted");

		// closing HTable
		table.close();
	}
}
