package edu.ucsb.rc.transactions;

import java.io.Serializable;
import java.util.HashMap;

public class Operation implements Serializable {
	private static final long serialVersionUID = 5935230338799222569L;

	public enum Type {
		READ, WRITE
	}
	
	private Type type;
	private String key;
	private HashMap<String, String> columnValues;
	private long timestamp;
	
	public Operation() {
		this.columnValues = new HashMap<String, String>();
	}
	
	public Type getType() {
		return type;
	}
	
	public void setType(Type type) {
		this.type = type;
	}
	
	public String getKey() {
		return key;
	}
	
	public void setKey(String key) {
		this.key = key;
	}

	public HashMap<String, String> getColumnValues() {
		return columnValues;
	}

	public void setColumnValues(HashMap<String, String> columnValues) {
		this.columnValues = columnValues;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
}
