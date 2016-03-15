package edu.ucsb.spanner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import edu.ucsb.spanner.network.NetworkHandler;
import edu.ucsb.spanner.network.NetworkHandlerInterface;

public class MultiDatacenter {
	private static MultiDatacenter _instance;
	private HashMap<Integer, Datacenter> datacenters;
	private Datacenter currentDatacenter;
	private Shard currentShard;
	private NetworkHandlerInterface networkHandler;
	
	private MultiDatacenter() {
		this.datacenters = new HashMap<Integer, Datacenter>();
	}
	
	public static MultiDatacenter getInstance() {
		if (_instance == null) {
			_instance = new MultiDatacenter();
		}
		return _instance;
	}
	
	public void addDatacenter(Datacenter dc) throws Exception {
		if (dc.getDatacenterID() < 0) {
			throw new Exception("The datacenter ID is not set. The datacenter cannot be added to the MultiDatacenter");
		}
		if (this.datacenters.containsKey(dc.getDatacenterID())) {
			throw new Exception("The datacenter ID already exists in the MultiDatacenter. The datacenter cannot be added to the MultiDatacenter");
		}
		this.datacenters.put(dc.getDatacenterID(), dc);
	}
	
	public ArrayList<Datacenter> getDatacenters() {
		return new ArrayList<Datacenter>(this.datacenters.values());
	}
	
	public void removeAllDatacenters() {
		this.datacenters.clear();
	}
	
	public Datacenter getDatacenter(int id) {
		return this.datacenters.containsKey(id) ? this.datacenters.get(id) : null;
	}
	
	public void setCurrentDatacenter(Datacenter dc) {
		this.currentDatacenter = dc;
	}
	
	public Datacenter getCurrentDatacenter() {
		return this.currentDatacenter;
	}
	
	public void setCurrentShard(Shard shard) {
		this.currentShard = shard;
	}
	
	public Shard getCurrentShard() {
		return this.currentShard;
	}
	
	public NetworkHandlerInterface getNetworkHandler() {
		return this.networkHandler;
	}
	
	public void setNetworkHandler(NetworkHandlerInterface networkHandler) {
		this.networkHandler = networkHandler;
	}
	
	public void initializeShards() {
		Collection<Datacenter> allDatacenters = this.datacenters.values();
		for (Datacenter dc : allDatacenters) {
			dc.initializeShards();
		}
	}
	
	public ArrayList<Shard> getOtherShardsWithId(int shardID) {
		ArrayList<Shard> allShards = new ArrayList<Shard>();
		
		Collection<Datacenter> allDatacenters = this.datacenters.values();
		for (Datacenter dc : allDatacenters) {
			if (this.currentDatacenter != dc) {
				allShards.add(dc.getShard(shardID));
			}
		}
		
		return allShards;
	}
	
	public void initializeDatastore() {
		Datastore datastore = Datastore.getInstance();
		datastore.initialize();
	}
}
