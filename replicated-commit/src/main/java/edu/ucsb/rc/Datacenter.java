package edu.ucsb.rc;

import java.util.ArrayList;

public class Datacenter {
	private int datacenterID;
	private ArrayList<Shard> shards;
	
	public Datacenter() {
		this.shards = new ArrayList<Shard>();
		this.setDatacenterID(-1);
	}
	
	public void addShard(Shard shard) {
		this.shards.add(shard);
	}
	
	public ArrayList<Shard> getShards() {
		return this.shards;
	}
	
	public Shard getShard(int id) {
		return id < this.shards.size() ? this.shards.get(id) : null;
	}

	public int getDatacenterID() {
		return datacenterID;
	}

	public void setDatacenterID(int datacenterID) {
		this.datacenterID = datacenterID;
	}
	
	public int getShardIdForKey(String key) {
		return key.hashCode() % 3;
	}
}
