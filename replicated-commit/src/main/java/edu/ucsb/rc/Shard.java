package edu.ucsb.rc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import edu.ucsb.rc.locks.LocksManager;
import edu.ucsb.rc.model.Message;
import edu.ucsb.rc.model.Operation;
import edu.ucsb.rc.model.Transaction;
import edu.ucsb.rc.network.NetworkHandler;
import edu.ucsb.rc.network.NetworkHandlerInterface;
import edu.ucsb.rc.protocols.PaxosAcceptsManager;
import edu.ucsb.rc.protocols.TwoPhaseCommitManager;

public class Shard {
	private final static Logger LOGGER = Logger.getLogger(Shard.class.getName());
	
	private int shardID;
	private String ipAddress;
	private Datacenter datacenter = null;
	private LocksManager locksManager;
	private TwoPhaseCommitManager twoPCmanager;
	private PaxosAcceptsManager paxosAcceptsManager;
	private CommitLogger commitLogger;
	
	public Shard() {
		this.locksManager = new LocksManager();
		this.setShardID(-1);
	}
	
	public void initializeShard() {
		this.twoPCmanager = new TwoPhaseCommitManager(this.datacenter.getShards().size());
		this.paxosAcceptsManager = new PaxosAcceptsManager(MultiDatacenter.getInstance().getDatacenters().size());
		this.commitLogger = new CommitLogger();
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
		
		ArrayList<Operation> readSet = t.getReadSet();
		boolean allSharedLocksAcquired = true;
		NetworkHandlerInterface networkHandler = MultiDatacenter.getInstance().getNetworkHandler();
		Message messageForClient = new Message();
		
		for (Operation op : readSet) {
			if (this.operationKeyBelongsToCurrentChard(op)) {
				if (allSharedLocksAcquired = this.locksManager.addSharedLock(op.getKey(), t.getServerTransactionId())) {
					// read value of key in datastore
					long timestampOfLastUpdate = Datastore.getInstance().read(op.getKey(), op.getColumnValues());
					op.setTimestamp(timestampOfLastUpdate);
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
			messageForClient.setMessageType(Message.MessageType.READ_FAILED);
		}
		
		messageForClient.setTransaction(t);
		networkHandler.sendMessageToClient(t, messageForClient);
	}
	
	public void handlePaxosAcceptRequest(Transaction t) {
		// Handle a PAXOS Accept request coming from client
		
		NetworkHandlerInterface networkHandler = MultiDatacenter.getInstance().getNetworkHandler();
		
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
		
		if (!this.locksManager.checkSharedLocksAreStillAcquiredForTxn(t)) {
			// Remove all shared locks for Txn
			this.locksManager.removeAllSharedLocksForTxn(t);
			
			this.sendMessageToOtherShard(shardSender, Message.MessageType.TWO_PHASE_COMMIT__PREPARE_DENIED, t);
			return;
		}
		if (!this.locksManager.acquireExclusiveLocksForTxn(t)) {
			// Remove all locks for Txn
			this.locksManager.removeAllLocksForTxn(t);
			
			this.sendMessageToOtherShard(shardSender, Message.MessageType.TWO_PHASE_COMMIT__PREPARE_DENIED, t);
			return;
		}
		
		// Remove all shared locks for Txn
		this.locksManager.removeAllSharedLocksForTxn(t);
		
		this.sendMessageToOtherShard(shardSender, Message.MessageType.TWO_PHASE_COMMIT__PREPARE_ACCEPTED, t);
	}
	
	public void handleTwoPhaseCommitPrepareAccepted(Transaction t, int shardIdOfSender) {
		/* We signal a new accepted 2PC prepare. If everybody has accepted, we send
		 * paxos accept accepted to client and all other coordinators in other datacenters.
		 */
		
		if (this.twoPCmanager.signalAcceptedPrepare(t)) {
			this.twoPCmanager.stopTracking2PCaccepts(t);
			
			boolean reachedMajorityPaxosAccepts = this.paxosAcceptsManager.increaseAcceptAccepted(t);
			
			ArrayList<Shard> coordinatorsInOtherDCs = MultiDatacenter.getInstance().getOtherShardsWithId(this.shardID);
			for (Shard otherCoordinator : coordinatorsInOtherDCs) {
				this.sendMessageToOtherShard(otherCoordinator, Message.MessageType.PAXOS__ACCEPT_REQUEST_ACCEPTED, t);
			}
			
			this.sendMessageToClient(Message.MessageType.PAXOS__ACCEPT_REQUEST_ACCEPTED, t);
			
			if (reachedMajorityPaxosAccepts) {
				// Transaction has to be committed
				this.start2PCcommitInDatacenter(t);
			}
		}
	}
	
	public void handleTwoPhaseCommitPrepareDenied(Transaction t, int shardIdOfSender) {
		this.twoPCmanager.stopTracking2PCaccepts(t);
		this.sendMessageToClient(Message.MessageType.PAXOS__ACCEPT_REQUEST_DENIED, t);
		
		// Is that necessary? Protocol doesn't mention that but could be helpful to release some memory
		ArrayList<Shard> coordinatorsInOtherDCs = MultiDatacenter.getInstance().getOtherShardsWithId(this.shardID);
		for (Shard otherCoordinator : coordinatorsInOtherDCs) {
			this.sendMessageToOtherShard(otherCoordinator, Message.MessageType.PAXOS__ACCEPT_REQUEST_DENIED, t);
		}
	}
	
	public void handlePaxosAcceptRequestAccepted(Transaction t, int shardIdOfSender) {
		/*
		 *  We increase number of accepts for that transaction. If we reached majority of accepts,
		 *  we start 2PC commit in datacenter (the transaction is committed).
		 */
		if (this.paxosAcceptsManager.increaseAcceptAccepted(t)) {
			// Transaction has to be committed
			this.start2PCcommitInDatacenter(t);
		}
	}
	
	public void start2PCcommitInDatacenter(Transaction t) {
		// We no longer need to track number of accepts
		this.paxosAcceptsManager.removeTrackOfPaxosAccepts(t);
		
		ArrayList<Shard> shardsInDatacenter = this.datacenter.getShards();
		for (Shard cohortShard : shardsInDatacenter) {
			this.sendMessageToOtherShard(cohortShard, Message.MessageType.TWO_PHASE_COMMIT__COMMIT, t);
		}
	}
	
	public void handleTwoPhaseCommitCommit(Transaction t, int shardIdOfSender) {
		Datastore datastore = Datastore.getInstance();
		ArrayList<Operation> writeSet = t.getWriteSet();
		
		for (Operation writeOp : writeSet) {
			if (writeOp.getShardIdHoldingData() == this.shardID) {
				datastore.write(writeOp.getKey(), writeOp.getColumnValues());
			}
		}
		
		this.locksManager.removeAllLocksForTxn(t);
		// Transaction is officially committed in this shard!!!
		this.commitLogger.logCommit(t);
	}
	
	public boolean operationKeyBelongsToCurrentChard(Operation op) {
		return this.datacenter.getShardIdForKey(op.getKey()) == this.shardID;
	}
	
	private void sendMessageToOtherShard(Shard shard, Message.MessageType messageType, Transaction t) {
		Message messageForShardSender = new Message();
		messageForShardSender.setMessageType(messageType);
		messageForShardSender.setShardIdOfSender(this.shardID);
		messageForShardSender.setTransaction(t);
		
		LOGGER.info("Send the messageType:" + messageType + " to shardID:" + shard.getShardID() + " of datacenterID:" + shard.getDatacenter().getDatacenterID() + " | serverTransactionID:" + t.getServerTransactionId());
		
		NetworkHandlerInterface networkHandler = MultiDatacenter.getInstance().getNetworkHandler();
		networkHandler.sendMessageToShard(shard, messageForShardSender);
	}
	
	private void sendMessageToClient(Message.MessageType messageType, Transaction t) {
		Message messageForClient = new Message();
		messageForClient.setMessageType(messageType);
		messageForClient.setShardIdOfSender(this.shardID);
		messageForClient.setTransaction(t);
		
		LOGGER.info("Send to client the messageType:" + messageType + " | clientTransactionID:" + t.getTransactionIdDefinedByClient() + " | serverTransactionID:" + t.getServerTransactionId());
		
		NetworkHandlerInterface networkHandler = MultiDatacenter.getInstance().getNetworkHandler();
		networkHandler.sendMessageToClient(t, messageForClient);
	}
}
