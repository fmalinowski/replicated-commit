package edu.ucsb.rc;

import java.util.HashMap;

public class Datastore {
	private static Datastore _instance = null;
	
	private Datastore() {
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
	 */
	public void read(String key, HashMap<String, String> columnValues) {
		// TODO
	}
	
	/*
	 *  This write the row associated with the key into the datastore
	 *  The keys of the HashMap represent the name of the columns 
	 */
	public void write(String key, HashMap<String, String> columnValues) {
		// TODO
	}
}
