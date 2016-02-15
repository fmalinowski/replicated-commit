package edu.ucsb.rc;

public class Shard {
	private int shardID;
	private String ipAddress;
	private Datacenter datacenter = null;
	
	public String getIpAddress() {
		return this.ipAddress;
	}
	
	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	public int getShardID() {
		return shardID;
	}

	public void setShardID(int shardID) {
		this.shardID = shardID;
	}

	public Datacenter getDatacenter() {
		return datacenter;
	}

	public void setDatacenter(Datacenter datacenter) {
		this.datacenter = datacenter;
	}
}
