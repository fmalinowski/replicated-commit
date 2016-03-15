package edu.ucsb.spanner;

import java.util.ArrayList;
import java.util.logging.Logger;

import edu.ucsb.spanner.locks.LocksManager;
import edu.ucsb.spanner.model.Message;
import edu.ucsb.spanner.model.Operation;
import edu.ucsb.spanner.model.Transaction;
import edu.ucsb.spanner.network.NetworkHandlerInterface;
import edu.ucsb.spanner.protocols.PaxosManager;
import edu.ucsb.spanner.protocols.TwoPhaseCommitCoordinator;

public class Shard {
	private final static Logger LOGGER = Logger
			.getLogger(Shard.class.getName());

	private int shardID;
	private String ipAddress;
	private Datacenter datacenter = null;
	private LocksManager locksManager;
	private TwoPhaseCommitCoordinator twoPCmanager;
	private PaxosManager paxosManager;
	private CommitLogger commitLogger;

	public Shard() {
		this.locksManager = new LocksManager();
		this.setShardID(-1);
	}

	public enum PaxosType {
		COMMIT_PREPARE,
		COMMIT_COMMIT
	}

	public void initializeShard() {
		this.twoPCmanager = new TwoPhaseCommitCoordinator();
		this.paxosManager = new PaxosManager(MultiDatacenter.getInstance()
				.getDatacenters().size());
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

	// Role as a Paxos Leader
	public void handleTwoPhaseCommitPrepareFromClient(Transaction transaction) {

		// acquireExcluiveLocks();
		// logTwoPhaseCommitPrepareLocally();
		// Talk to others peer shards (or cohorts)
		startPaxos("peersOfPaxosLeaders", PaxosType.COMMIT_PREPARE);

	}

	//Role as Paxos Leader
	private void startPaxos(String string, PaxosType type) {
		
		if (PaxosType.COMMIT_PREPARE == type) {
			paxosManager.setUpContext();

			sendPaxosMessages();
		}

	}

	//Role as a Shard
	public void handlePaxosPrepare(Transaction transaction, PaxosType type, int shardIdOfSender) {
		
		if(PaxosType.COMMIT_PREPARE == type)
		{
				
		}
		else if(PaxosType.COMMIT_COMMIT == type)
		{
			
		}

	}

	//Role as Paxos Leader
	public void handlePaxosPromise(Transaction transaction, PaxosType type, int shardIdOfSender) {
		if(PaxosType.COMMIT_PREPARE == type)
		{
				
		}
		else if(PaxosType.COMMIT_COMMIT == type)
		{
			
		}

	}

	public void handlePaxosAccept(Transaction transaction, int shardIdOfSender) {
		// TODO Auto-generated method stub

	}

	// Role = 2PC Coordinator
	public void handlePaxosAcceptAccepted(Transaction transaction,
			int shardIdOfSender) {

		releaseLocks();
		sendMessageToClient(null, t);

		// send messages to leaders to perform Paxos based Commit

	}

	public void handlePaxosAcceptRejected(Transaction transaction,
			int shardIdOfSender) {
		// TODO Auto-generated method stub

	}

	// Role : Cohorts
	public void handleTwoPhaseCommitPrepareFromPaxosLeader() {
		// Basically check if TwoPhaseCommit is possible for this shard
		// Send the message to Paxos Leaders as to whether you can do it or not

	}

	// Role as a 2PC Coordinator
	public void handleTwoPhaseCommitPrepareAccepted(Transaction t,
			int shardIdOfSender) {

		logTwoPhaseCommitLocallyAsACoordinator();
		replicateLogEntryOf2PCWithPaxos();
	}

	// Role as a 2PC Coordinator
	private void logTwoPhaseCommitLocallyAsACoordinator() {
		// TODO Auto-generated method stub

	}

	// Role as 2PC Coordinator
	public void handleTwoPhaseCommitPrepareDenied(Transaction t,
			int shardIdOfSender) {

	}

	// Role as 2PC Coordinator
	private void replicateLogEntryOf2PCWithPaxos() {

		// Use Paxos Manager to have 2PC done via Paxos Majority
		// send Paxos Request etc (go to netwrork)
		// receivers are all the leader homies

	}

	// As a Paxos Leader
	public void handleTwoPhaseCommitCommit(Transaction t, int shardIdOfSender) {
		// Start Paxos and replicate Commit
	}

	public boolean operationKeyBelongsToCurrentShard(Operation op) {
		return this.datacenter.getShardIdForKey(op.getKey()) == this.shardID;
	}

	private void sendMessageToOtherShard(Shard shard,
			Message.MessageType messageType, Transaction t) {
		Message messageForShardSender = new Message();
		messageForShardSender.setMessageType(messageType);
		messageForShardSender.setShardIdOfSender(this.shardID);
		messageForShardSender.setTransaction(t);

		LOGGER.info("Send the messageType:" + messageType + " to shardID:"
				+ shard.getShardID() + " of datacenterID:"
				+ shard.getDatacenter().getDatacenterID()
				+ " | serverTransactionID:" + t.getServerTransactionId());

		NetworkHandlerInterface networkHandler = MultiDatacenter.getInstance()
				.getNetworkHandler();
		networkHandler.sendMessageToShard(shard, messageForShardSender);
	}

	private void sendMessageToClient(Message.MessageType messageType,
			Transaction t) {
		Message messageForClient = new Message();
		messageForClient.setMessageType(messageType);
		messageForClient.setShardIdOfSender(this.shardID);
		messageForClient.setTransaction(t);

		LOGGER.info("Send to client the messageType:" + messageType
				+ " | clientTransactionID:"
				+ t.getTransactionIdDefinedByClient()
				+ " | serverTransactionID:" + t.getServerTransactionId());

		NetworkHandlerInterface networkHandler = MultiDatacenter.getInstance()
				.getNetworkHandler();
		networkHandler.sendMessageToClient(t, messageForClient);
	}

}
