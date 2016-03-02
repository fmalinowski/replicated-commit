package edu.ucsb.rc;

import java.io.IOException;
import java.util.HashMap;

import edu.ucsb.rc.dao.HBaseDAO;

public class Datastore {
	private static Datastore _instance = null;
	private HBaseDAO hbaseDao;
	private final String table = "usertable";
	private final String columnFamily = "cf";
	
	protected Datastore() {
	}
	
	public static Datastore getInstance() {
		if (_instance == null) {
			_instance = new Datastore();
		}
		return _instance;
	}
	
	public void initialize() {
		this.hbaseDao = new HBaseDAO();
		// We need to create the table here!
		String[] columnFamilies = {this.columnFamily};
		try {
			this.hbaseDao.createTable(this.table, columnFamilies);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/*
	 *  This reads the row associated with the key from the datastore
	 *  The keys of the HashMap represent the name of the columns
	 *  It returns the timestamp of the last update of that row in HBase 
	 */
	public long read(String key, HashMap<String, String> columnValues) {		
		// We need to return the timestamp of the last update of that row in HBase
		
		try {
			return this.hbaseDao.getValuesOfOneRecord(this.table, key, this.columnFamily, columnValues);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return -1;
	}
	
	/*
	 *  This write the row associated with the key into the datastore
	 *  The keys of the HashMap represent the name of the columns 
	 */
	public void write(String key, HashMap<String, String> columnValues) {
		try {
			this.hbaseDao.addRecordWithSeveralQualifiers(this.table, key, this.columnFamily, columnValues);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
