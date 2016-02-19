package edu.ucsb.rc;

import java.util.HashMap;

import edu.ucsb.rc.locks.LocksManager;
import edu.ucsb.rc.transactions.Transaction;

public class Shard {
	private int shardID;
	private String ipAddress;
	private Datacenter datacenter = null;
	private HashMap<String, Transaction> transactionsMap;
	private LocksManager locksManager;
	
	public Shard() {
		this.transactionsMap = new HashMap<String, Transaction>();
		this.locksManager = new LocksManager();
	}
	
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
	
	public void handleReadRequestFromClient(Transaction t) {
		// Handle a read request coming from a client
		this.addTransaction(t);
		// TODO
	}
	
	public void handlePaxosAcceptRequest(Transaction t) {
		// Handle a PAXOS Accept request coming from client
		if (!this.containsTransaction(t.getServerTransactionId())) {
			this.addTransaction(t);
		}
		// TODO
	}
	
	private void addTransaction(Transaction t) {
		this.transactionsMap.put(t.getServerTransactionId(), t);
	}
	
	private boolean containsTransaction(String serverTransactionId) {
		return this.transactionsMap.containsKey(serverTransactionId);
	}
	
	private Transaction getTransaction(String serverTransactionId) {
		return this.transactionsMap.containsKey(serverTransactionId) ? 
				this.transactionsMap.get(serverTransactionId) : null;
	}
}
