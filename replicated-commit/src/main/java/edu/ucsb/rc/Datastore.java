package edu.ucsb.rc;

import java.util.HashMap;

public class Datastore {
	private static Datastore _instance = null;
	
	protected Datastore() {
	}
	
	public static Datastore getInstance() {
		if (_instance == null) {
			_instance = new Datastore();
		}
		return _instance;
	}

	/*
	 *  This reads the row associated with the key from the datastore
	 *  The keys of the HashMap represent the name of the columns
	 *  It returns the timestamp of the last update of that row in HBase 
	 */
	public int read(String key, HashMap<String, String> columnValues) {
		// TODO
		
		// We need to return the timestamp of the last update of that row in HBase
		return -1;
	}
	
	/*
	 *  This write the row associated with the key into the datastore
	 *  The keys of the HashMap represent the name of the columns 
	 */
	public void write(String key, HashMap<String, String> columnValues) {
		// TODO
	}
}
