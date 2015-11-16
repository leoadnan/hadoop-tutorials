package basicOps;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class Ex8_DeleteData {

	public static void main(String[] args) throws IOException {

		// Instantiating configuration class
		Configuration config = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(config);

		// Get HbaseAdmin class
		Admin admin = connection.getAdmin();

		// Instantiating HTable class
		Table table = connection.getTable(TableName.valueOf("emp"));

		// Instantiating Delete class
		Delete delete = new Delete(Bytes.toBytes("row1"));
		delete.addColumn(Bytes.toBytes("personal data"), Bytes.toBytes("name"));
		delete.addFamily(Bytes.toBytes("professional data"));

		// deleting the data
		table.delete(delete);

		// closing the HTable object
		table.close();
		System.out.println("data deleted.....");
	}
}
