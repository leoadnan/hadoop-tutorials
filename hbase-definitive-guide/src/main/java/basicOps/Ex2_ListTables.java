package basicOps;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class Ex2_ListTables {

	public static void main(String[] args) throws IOException {
		
		// Instantiating configuration class
		Configuration config = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(config);

		// Get HbaseAdmin class
		Admin admin = connection.getAdmin();

		// Getting all the list of tables using HBaseAdmin object
		HTableDescriptor[] tableDescriptor = admin.listTables();

		// printing all the table names.
		for (int i = 0; i < tableDescriptor.length; i++) {
			System.out.println(tableDescriptor[i].getNameAsString());
		}
	}

}
