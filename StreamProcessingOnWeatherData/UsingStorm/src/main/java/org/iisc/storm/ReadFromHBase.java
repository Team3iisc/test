package org.iisc.storm;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
/*
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.TableName;
*/
public class ReadFromHBase {/*


	public static void main(String[] args) throws IOException, Exception{
		System.out.println("Starting Execution");
		// Instantiating configuration class
		Configuration con = HBaseConfiguration.create();

		// Instantiating HbaseAdmin class
		HBaseAdmin admin = new HBaseAdmin(con);

		// Instantiating table descriptor class
		HTableDescriptor tableDescriptor = new HTableDescriptor("emp");

		// Adding column families to table descriptor
		tableDescriptor.addFamily(new HColumnDescriptor("personal"));
		tableDescriptor.addFamily(new HColumnDescriptor("professional"));

		// Execute the table through admin
		admin.createTable(tableDescriptor);
		System.out.println(" Table created ");


		

		// Instantiating Configuration class
		Configuration config = HBaseConfiguration.create();

		// Instantiating HTable class
		HTable table = new HTable(config, "envsensor");

		// Instantiating Get class
		Get g = new Get(Bytes.toBytes("row1"));

		// Reading the data		
		Result result = table.get(g);


		// Reading values from Result class object
		byte [] value = result.getValue(Bytes.toBytes(""),Bytes.toBytes("temperature"));

		byte [] value1 = result.getValue(Bytes.toBytes(""),Bytes.toBytes("humidity"));

		// Printing the values
		String name = Bytes.toString(value);
		String city = Bytes.toString(value1);

		System.out.println("temp: " + name + " humidity: " + city);
		 
	}
*/}
