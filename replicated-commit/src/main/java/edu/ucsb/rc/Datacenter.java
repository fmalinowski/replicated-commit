package edu.ucsb.rc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

public class Datacenter {
	private int datacenterID;
	private HashMap<Integer, Shard> shards;
	
	public Datacenter() {
		this.shards = new HashMap<Integer, Shard>();
		this.setDatacenterID(-1);
	}
	
	public void initializeShards() {
		Collection<Shard> allShards = this.shards.values();
		for (Shard shard : allShards) {
			shard.initializeShard();
		}
	}
	
	public void addShard(Shard shard) throws Exception {
		if (shard.getShardID() < 0) {
			throw new Exception("The shard ID is not set. The shard cannot be added to the Datacenter");
		}
		if (this.shards.containsKey(shard.getShardID())) {
			throw new Exception("The shard ID already exists in the Datacenter. The shard cannot be added to the Datacenter");
		}
		this.shards.put(shard.getShardID(), shard);
		shard.setDatacenter(this);
	}
	
	public ArrayList<Shard> getShards() {
		return new ArrayList<Shard>(this.shards.values());
	}
	
	public Shard getShard(int id) {
		return this.shards.containsKey(id) ? this.shards.get(id) : null;
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
