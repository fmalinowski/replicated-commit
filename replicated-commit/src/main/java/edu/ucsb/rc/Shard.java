package edu.ucsb.rc;

import java.util.ArrayList;
import java.util.HashMap;

import edu.ucsb.rc.locks.LocksManager;
import edu.ucsb.rc.network.NetworkHandler;
import edu.ucsb.rc.transactions.Operation;
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
		
		ArrayList<Operation> readSet = t.getReadSet();
		boolean allSharedLocksAcquired = true;
		NetworkHandler networkHandler = MultiDatacenter.getInstance().getNetworkHandler();
		Message messageForClient = new Message();
		
		for (Operation op : readSet) {
			if (this.operationKeyBelongsToCurrentChard(op)) {
				if (allSharedLocksAcquired = this.locksManager.addSharedLock(op.getKey(), t.getServerTransactionId())) {
					// read value of key in datastore
					Datastore.getInstance().read(op.getKey(), op.getColumnValues());
				} else {
					break;
				}
			}
		}
		
		if (allSharedLocksAcquired) {
			messageForClient.setMessageType(Message.MessageType.READ_ANSWER);
		} else {
			// We remove all locks and remnove the transaction from the current transactions
			// (the transaction is aborted)
			for (Operation op : readSet) {
				if (this.operationKeyBelongsToCurrentChard(op)) {
					this.locksManager.removeLock(op.getKey(), t.getServerTransactionId());
				}
			}
			this.removeTransaction(t);
			messageForClient.setMessageType(Message.MessageType.READ_FAILED);
		}
		
		messageForClient.setTransaction(t);
		networkHandler.sendMessageToClient(t, messageForClient);
	}
	
	public void handlePaxosAcceptRequest(Transaction t) {
		// Handle a PAXOS Accept request coming from client
		if (!this.containsTransaction(t.getServerTransactionId())) {
			this.addTransaction(t);
		}
		// TODO
	}
	
	public void addTransaction(Transaction t) {
		this.transactionsMap.put(t.getServerTransactionId(), t);
	}
	
	public boolean containsTransaction(String serverTransactionId) {
		return this.transactionsMap.containsKey(serverTransactionId);
	}
	
	public Transaction getTransaction(String serverTransactionId) {
		return this.transactionsMap.containsKey(serverTransactionId) ? 
				this.transactionsMap.get(serverTransactionId) : null;
	}
	
	public void removeTransaction(Transaction t) {
		if (this.containsTransaction(t.getServerTransactionId())) {
			this.transactionsMap.remove(t.getServerTransactionId());
		}
	}
	
	public boolean operationKeyBelongsToCurrentChard(Operation op) {
		return this.datacenter.getShardIdForKey(op.getKey()) == this.shardID;
	}
}
