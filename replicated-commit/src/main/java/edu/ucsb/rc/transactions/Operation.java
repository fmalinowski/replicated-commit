package edu.ucsb.rc.transactions;

import java.io.Serializable;
import java.util.HashMap;

public class Operation implements Serializable {
	@Override
	public String toString() {
		return "Operation [type=" + type + ", key=" + key + ", columnValues="
				+ columnValues + ", timestamp=" + timestamp + "]";
	}

	private static final long serialVersionUID = 5935230338799222569L;

	public enum Type {
		READ, WRITE
	}
	
	private static int numberOfShardsPerDatacenter = 3;
	
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
	
	public static void setShardsNumberPerDatacenter(int numberOfShardsPerDatacenter) {
		Operation.numberOfShardsPerDatacenter = numberOfShardsPerDatacenter;
	}
	
	public static int getShardsNumberPerDatacenter() {
		return Operation.numberOfShardsPerDatacenter;
	}
	
	public static int getShardIdHoldingData(String key) {
		if (key == null) {
			return -1;
		}
		return (key.hashCode() % Operation.numberOfShardsPerDatacenter);
	}
	
	public int getShardIdHoldingData() {
		return Operation.getShardIdHoldingData(this.key);
	}
}
