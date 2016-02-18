package edu.ucsb.rc;

import java.util.ArrayList;
import java.util.HashMap;

public class MultiDatacenter {
	private static MultiDatacenter _instance;
	private ArrayList<Datacenter> datacenters;
	private Datacenter currentDatacenter;
	private Shard currentShard;
	private HashMap<String, TransactionClient> transactionClientsMap;
	
	private MultiDatacenter() {
		this.datacenters = new ArrayList<Datacenter>();
		this.transactionClientsMap = new HashMap<String, TransactionClient>();
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
	
	public void addTransactionClient(TransactionClient tc) {
		this.transactionClientsMap.put(tc.getServerSideTransactionID(), tc);
	}
	
	public boolean containsTransactionClient(String serverSideTransactionId) {
		return this.transactionClientsMap.containsKey(serverSideTransactionId);
	}
	
	public TransactionClient getTransactionClient(String serverSideTransactionId) {
		return this.transactionClientsMap.containsKey(serverSideTransactionId) ? 
				this.transactionClientsMap.get(serverSideTransactionId) : null;
	}
}
