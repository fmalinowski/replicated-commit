package edu.ucsb.rc;

import java.util.ArrayList;
import java.util.HashMap;

import edu.ucsb.rc.locks.LocksManager;
import edu.ucsb.rc.network.Message;
import edu.ucsb.rc.network.NetworkHandler;
import edu.ucsb.rc.protocols.TwoPhaseCommitManager;
import edu.ucsb.rc.transactions.Operation;
import edu.ucsb.rc.transactions.Transaction;

public class Shard {
	private int shardID;
	private String ipAddress;
	private Datacenter datacenter = null;
	private HashMap<String, Transaction> transactionsMap;
	private LocksManager locksManager;
	private TwoPhaseCommitManager twoPCmanager;
	
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
		this.twoPCmanager = new TwoPhaseCommitManager(this.datacenter.getShards().size());
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
			// We remove all locks and remove the transaction from the current transactions
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
		this.addTransaction(t);
		
		NetworkHandler networkHandler = MultiDatacenter.getInstance().getNetworkHandler();
		
		Message messageForShards = new Message();
		messageForShards.setShardIdOfSender(this.getShardID());
		messageForShards.setMessageType(Message.MessageType.TWO_PHASE_COMMIT__PREPARE);
		messageForShards.setTransaction(t);
		
		/*
		 *  Start tracking number of accepted 2PC requests on a coordinator site
		 *  and also records timestamp of when request was sent to remove some old
		 *  2PC requests to avoid using too much memory
		 */
		this.twoPCmanager.startTracking2PCaccepts(t);
		
		ArrayList<Shard> datacenterShards = this.datacenter.getShards();
		for (Shard datacenterShard : datacenterShards) {
			networkHandler.sendMessageToShard(datacenterShard, messageForShards);
		}
	}
	
	public void handleTwoPhaseCommitPrepare(Transaction t, int shardIdOfSender) {
		// Handle a 2PC prepare request
		Shard shardSender = this.datacenter.getShard(shardIdOfSender);
		
		if (!this.checkSharedLocksAreStillAcquiredForTxn(t)) {
			// Remove all shared locks for Txn
			this.removeAllSharedLocksForTxn(t);
			
			this.sendMessageToOtherShard(shardSender, Message.MessageType.TWO_PHASE_COMMIT__PREPARE_DENIED, t);
			return;
		}
		if (!this.acquireExclusiveLocksForTxn(t)) {
			// Remove all locks for Txn
			this.removeAllLocksForTxn(t);
			
			this.sendMessageToOtherShard(shardSender, Message.MessageType.TWO_PHASE_COMMIT__PREPARE_DENIED, t);
			return;
		}
		
		// Remove all shared locks for Txn
		this.removeAllSharedLocksForTxn(t);
		
		this.sendMessageToOtherShard(shardSender, Message.MessageType.TWO_PHASE_COMMIT__PREPARE_ACCEPTED, t);
	}
	
	public boolean checkSharedLocksAreStillAcquiredForTxn(Transaction t) {
		ArrayList<Operation> readSet = t.getReadSet();
		
		for (Operation readOp : readSet) {
			if (!this.locksManager.isLockedByTransaction(readOp.getKey(), t.getServerTransactionId())) {
				return false;
			}
		}
		return true;
	}
	
	public boolean acquireExclusiveLocksForTxn(Transaction t) {
		ArrayList<Operation> writeSet = t.getWriteSet();
		
		for (Operation writeOp : writeSet) {
			if (!this.locksManager.addExclusiveLock(writeOp.getKey(), t.getServerTransactionId())) {
				return false;
			}
		}
		return true;
	}
	
	public void removeAllSharedLocksForTxn(Transaction t) {
		ArrayList<Operation> readSet = t.getReadSet();
		
		for (Operation readOp : readSet) {
			this.locksManager.removeLock(readOp.getKey(), t.getServerTransactionId());
		}
	}
	
	public void removeAllLocksForTxn(Transaction t) {
		this.removeAllSharedLocksForTxn(t);
		ArrayList<Operation> writeSet = t.getWriteSet();
		
		for (Operation writeOp : writeSet) {
			this.locksManager.removeLock(writeOp.getKey(), t.getServerTransactionId());
		}
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
	
	private void removeTransaction(Transaction t) {
		if (this.containsTransaction(t.getServerTransactionId())) {
			this.transactionsMap.remove(t.getServerTransactionId());
		}
	}
	
	public boolean operationKeyBelongsToCurrentChard(Operation op) {
		return this.datacenter.getShardIdForKey(op.getKey()) == this.shardID;
	}
	
	private void sendMessageToOtherShard(Shard shard, Message.MessageType messageType, Transaction t) {
		Message messageForShardSender = new Message();
		messageForShardSender.setMessageType(messageType);
		messageForShardSender.setShardIdOfSender(this.shardID);
		messageForShardSender.setTransaction(t);
		
		NetworkHandler networkHandler = MultiDatacenter.getInstance().getNetworkHandler();
		networkHandler.sendMessageToShard(shard, messageForShardSender);
	}
}
