package edu.ucsb.rc.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDAO {

	private Configuration config;

	public HBaseDAO() {
		System.out.println("Connecting to HBase Server ...");
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "127.0.0.1");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("hbase.master", "127.0.0.1:60010");
		try {
			HBaseAdmin.checkHBaseAvailable(config);
			System.out.println("Conncted to HBase Server ..");

		} catch (MasterNotRunningException e) {
			System.out.println("HBase Master is not running..");
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			System.out
					.println("There was an issue with ZooKeeper connection..");
			e.printStackTrace();
		} catch (Exception e) {
			System.out
					.println("There was an issue connecting to HBase server..");
			e.printStackTrace();
		}

	}

	public void createTable(String tableName, String[] familys)
			throws IOException {

		HBaseAdmin admin = new HBaseAdmin(config);

		if (admin.tableExists(tableName)) {
			System.out.println("table already exists!");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			for (int i = 0; i < familys.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(familys[i]));
			}
			admin.createTable(tableDesc);
			System.out.println("create table " + tableName + " ok.");
		}

		admin.close();
	}

	/**
	 * Delete a table
	 */
	public void deleteTable(String tableName) throws Exception {

		HBaseAdmin admin = new HBaseAdmin(config);
		try {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println("delete table " + tableName + " ok.");
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
		admin.close();
	}

	/**
	 * Put (or insert) a row
	 */
	public void addRecord(String tableName, String rowKey, String family,
			String qualifier, String value) throws Exception {

		HTable table = new HTable(config, tableName);
		try {
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),
					Bytes.toBytes(value));
			table.put(put);
			System.out.println("insert recored " + rowKey + " to table "
					+ tableName + " ok.");
		} catch (IOException e) {
			e.printStackTrace();
		}
		table.close();

	}
	
	public void addRecordWithSeveralQualifiers(String tableName, String rowKey, String family,
			HashMap<String, String> qualifierValues) throws Exception {

		HTable table = new HTable(config, tableName);
		
		Set<String> qualifiers = qualifierValues.keySet();
		Put put = new Put(Bytes.toBytes(rowKey));
		
		for (String qualifier : qualifiers) {
			String qualifierValue = qualifierValues.get(qualifier);
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),
					Bytes.toBytes(qualifierValue));
		}
		
		table.put(put);
		table.close();
	}

	/**
	 * Delete a row
	 */
	public void delRecord(String tableName, String rowKey) throws IOException {
		HTable table = new HTable(config, tableName);
		List<Delete> list = new ArrayList<Delete>();
		Delete del = new Delete(rowKey.getBytes());
		list.add(del);
		table.delete(list);
		System.out.println("del recored " + rowKey + " ok.");
		table.close();
	}

	/**
	 * Get a row
	 */
	public void getOneRecord(String tableName, String rowKey)
			throws IOException {
		HTable table = new HTable(config, tableName);
		Get get = new Get(rowKey.getBytes());
		Result rs = table.get(get);
		for (KeyValue kv : rs.raw()) {
			System.out.print(new String(kv.getRow()) + " ");
			System.out.print(new String(kv.getFamily()) + ":");
			System.out.print(new String(kv.getQualifier()) + " ");
			System.out.print(kv.getTimestamp() + " ");
			System.out.println(new String(kv.getValue()));
		}
		table.close();
	}
	
	public long getValuesOfOneRecord(String tableName, String rowKey, String family, 
			HashMap<String, String> qualifierValues) throws IOException {
		long mostRecentTimestamp = -1;
		
		HTable table = new HTable(config, tableName);
		Get get = new Get(rowKey.getBytes());
		Result rs = table.get(get);
		
		Set<String> keys = qualifierValues.keySet();
		for (String key : keys) {
			KeyValue valueForKey = rs.getColumnLatest(family.getBytes(), key.getBytes());
			qualifierValues.put(key, new String(valueForKey.getValue()));
			
			long timestampOfQualifier = valueForKey.getTimestamp();
			
			if (timestampOfQualifier > mostRecentTimestamp) {
				mostRecentTimestamp = timestampOfQualifier;
			}
		}
		table.close();
		return mostRecentTimestamp;
	}

	/**
	 * Scan (or list) a table
	 */
	public void getAllRecord(String tableName) {

		HTable table;
		try {
			table = new HTable(config, tableName);
			Scan s = new Scan();
			ResultScanner ss = table.getScanner(s);
			for (Result r : ss) {
				for (KeyValue kv : r.raw()) {
					System.out.print(new String(kv.getRow()) + " ");
					System.out.print(new String(kv.getFamily()) + ":");
					System.out.print(new String(kv.getQualifier()) + " ");
					System.out.print(kv.getTimestamp() + " ");
					System.out.println(new String(kv.getValue()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} 

	}

}
