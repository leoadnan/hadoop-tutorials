package ch07;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class Ex2_Importer {

    public static void main(String[] args) throws Exception {
        
        String [] pages = {"/", "/a.html", "/b.html", "/c.html"};
        
		Configuration conf = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("access_logs"));

        int totalRecords = 100000;
        int maxID = totalRecords / 1000;
        Random rand = new Random();
        System.out.println("importing " + totalRecords + " records ....");
        for (int i=0; i < totalRecords; i++)
        {
            int userID = rand.nextInt(maxID) + 1;
            byte [] rowkey = Bytes.add(Bytes.toBytes(userID), Bytes.toBytes(i));
            String randomPage = pages[rand.nextInt(pages.length)];
            Put put = new Put(rowkey);
            put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("page"), Bytes.toBytes(randomPage));
            table.put(put);
        }
        table.close();
        System.out.println("done");
    }
}