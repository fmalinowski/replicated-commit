package edu.ucsb.rc;

import java.util.ArrayList;

import edu.ucsb.rc.network.NetworkHandler;

public class MultiDatacenter {
	private static MultiDatacenter _instance;
	private ArrayList<Datacenter> datacenters;
	private Datacenter currentDatacenter;
	private Shard currentShard;
	private NetworkHandler networkHandler;
	
	private MultiDatacenter() {
		this.datacenters = new ArrayList<Datacenter>();
	}
	
	public static MultiDatacenter getInstance() {
		if (_instance == null) {
			_instance = new MultiDatacenter();
		}
		return _instance;
	}
	
	public void addDatacenter(Datacenter dc) {
		this.datacenters.add(dc);
	}
	
	public ArrayList<Datacenter> getDatacenters() {
		return this.datacenters;
	}
	
	public Datacenter getDatacenter(int id) {
		return id < this.datacenters.size() ? this.datacenters.get(id) : null;
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
	
	public NetworkHandler getNetworkHandler() {
		return this.networkHandler;
	}
	
	public void setNetworkHandler(NetworkHandler networkHandler) {
		this.networkHandler = networkHandler;
	}
}
