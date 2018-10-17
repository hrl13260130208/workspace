package com.test.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;







public class ClientTest {

	public static void main(String[] args) throws IOException {
		
		Configuration configuration=HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", "192.168.56.150");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		
		Connection connection=ConnectionFactory.createConnection(configuration);
		
		Admin admin=connection.getAdmin();
		/*
		HTableDescriptor[] table=admin.listTables();
		
		for (int i = 0; i < table.length; i++) {
			System.out.println(table[i].getNameAsString());
			
		}
	
		
		HTableDescriptor hTableDescriptor=new HTableDescriptor(TableName.valueOf("ClientTable"));
		hTableDescriptor.addFamily(new HColumnDescriptor("column1"));
		
		admin.createTable(hTableDescriptor);
		*/
		Put put=new Put(Bytes.toBytes("rowKey"));
		Table table=connection.getTable(TableName.valueOf("ClientTable"));
		
		HColumnDescriptor[] column=table.getTableDescriptor().getColumnFamilies();
		for (int i = 0; i < column.length; i++) {
			System.out.println(column[i].getNameAsString());
			put.addColumn(column[i].getName(), Bytes.toBytes("lie1"), Bytes.toBytes("test1"));
		}
		
		table.put(put);
		System.out.println("dddd");
		
		
		
		
	}
}
