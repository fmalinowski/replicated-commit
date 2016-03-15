package edu.ucsb.spanner;

import static edu.ucsb.spanner.Shard.PaxosType.COMMIT_LOG_REPLICATION;
import static edu.ucsb.spanner.model.Message.MessageType.PAXOS_PREPARE_COMMIT;
import static edu.ucsb.spanner.model.Message.MessageType.PAXOS_PREPARE_COMMIT_PREPARE;
import static edu.ucsb.spanner.model.Message.MessageType.PAXOS_PREPARE_REPLICATE_LOG;
import static edu.ucsb.spanner.model.Message.MessageType.PAXOS_PROMISE_COMMIT;
import static edu.ucsb.spanner.model.Message.MessageType.PAXOS_PROMISE_COMMIT_PREPARE;
import static edu.ucsb.spanner.model.Message.MessageType.PAXOS_PROMISE_REPLICATE_LOG;
import static edu.ucsb.spanner.model.Message.MessageType.PAXOS__ACCEPT_REQUEST_ACCEPTED_COMMIT;
import static edu.ucsb.spanner.model.Message.MessageType.PAXOS__ACCEPT_REQUEST_ACCEPTED_COMMIT_PREPARE;
import static edu.ucsb.spanner.model.Message.MessageType.PAXOS__ACCEPT_REQUEST_ACCEPTED_REPLICATE_LOG;
import static edu.ucsb.spanner.model.Message.MessageType.PAXOS__ACCEPT_REQUEST_COMMIT;
import static edu.ucsb.spanner.model.Message.MessageType.PAXOS__ACCEPT_REQUEST_COMMIT_PREPARE;
import static edu.ucsb.spanner.model.Message.MessageType.PAXOS__ACCEPT_REQUEST_DENIED_COMMIT;
import static edu.ucsb.spanner.model.Message.MessageType.PAXOS__ACCEPT_REQUEST_DENIED_COMMIT_PREPARE;
import static edu.ucsb.spanner.model.Message.MessageType.PAXOS__ACCEPT_REQUEST_DENIED_REPLICATE_LOG;
import static edu.ucsb.spanner.model.Message.MessageType.PAXOS__ACCEPT_REQUEST_REPLICATE_LOG;

import java.util.logging.Logger;

import edu.ucsb.spanner.locks.LocksManager;
import edu.ucsb.spanner.model.Message;
import edu.ucsb.spanner.model.Message.MessageType;
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
		COMMIT_PREPARE, COMMIT_LOG_REPLICATION, COMMIT_COMMIT
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

	// Role as Paxos Leader
	private void startPaxos(String string, PaxosType type) {

		if (PaxosType.COMMIT_PREPARE == type) {
			paxosManager.startNewPaxosSession();
			// sendPaxosMessages();
		} else if (PaxosType.COMMIT_LOG_REPLICATION == type) {
			paxosManager.startNewPaxosSession();
			//send msg to paxos leaders
		} else if (PaxosType.COMMIT_COMMIT == type) {
			paxosManager.startNewPaxosSession();
			//Send 
		}

	}

	// Role as a Shard (sent by leader)
	public void handlePaxosPrepare(Transaction transaction, MessageType type,
			int shardIdOfSender) {

		if (PAXOS_PREPARE_COMMIT_PREPARE == type) {

		} else if (PAXOS_PREPARE_COMMIT == type) {

		} else if (PAXOS_PREPARE_REPLICATE_LOG == type) {

		}

	}

	// Role as Paxos Leader (sent by shards)
	public void handlePaxosPromise(Transaction transaction, MessageType type,
			int shardIdOfSender) {

		if (PAXOS_PROMISE_COMMIT_PREPARE == type) {

		} else if (PAXOS_PROMISE_COMMIT == type) {

		} else if (PAXOS_PROMISE_REPLICATE_LOG == type) {

		}

	}

	// Role as Shards (sent by leader)
	public void handlePaxosAccept(Transaction transaction, MessageType type,
			int shardIdOfSender) {

		if (PAXOS__ACCEPT_REQUEST_COMMIT_PREPARE == type) {

		} else if (PAXOS__ACCEPT_REQUEST_COMMIT == type) {

		} else if (PAXOS__ACCEPT_REQUEST_REPLICATE_LOG == type) {

		}

	}

	// Role as Leader (sent by Shards)
	public void handlePaxosAcceptAccepted(Transaction transaction,
			MessageType type, int shardIdOfSender) {

		if (PAXOS__ACCEPT_REQUEST_ACCEPTED_COMMIT_PREPARE == type) {
			// Tell the coordinator that majority of shards are ready for
			// Commit 2PC
			// send message via 2PC Accept to 2PC leader
		} else if (PAXOS__ACCEPT_REQUEST_ACCEPTED_COMMIT == type) {
			//DataStore Commit the log
			

		} else if (PAXOS__ACCEPT_REQUEST_ACCEPTED_REPLICATE_LOG == type) {
			// releaseLocks();
			//TWO_PHASE_COMMIT__SUCCESS,
			sendMessageToClient(null, transaction);
			//send messages to leaders to perform Paxos based Commit
			//TWO_PHASE_COMMIT__COMMIT,
			

		}
	}

	// Role as Leader (sent by Shards)
	public void handlePaxosAcceptRejected(Transaction transaction,
			MessageType type, int shardIdOfSender) {

		if (PAXOS__ACCEPT_REQUEST_DENIED_COMMIT_PREPARE == type) {

		} else if (PAXOS__ACCEPT_REQUEST_DENIED_COMMIT == type) {
			///TWO_PHASE_COMMIT_FAILED,
		} else if (PAXOS__ACCEPT_REQUEST_DENIED_REPLICATE_LOG == type) {
			
			//TWO_PHASE_COMMIT_FAILED,

		}
	}

	// Role as a 2PC Coordinator received from leaders
	public void handleTwoPhaseCommitPrepareAccepted(Transaction t,
			int shardIdOfSender) {

		logTwoPhaseCommitLocallyAsACoordinator();
		//send log along with it
		startPaxos("", COMMIT_LOG_REPLICATION);
	}

	// Role as 2PC Coordinator
	public void handleTwoPhaseCommitPrepareDenied(Transaction t,
			int shardIdOfSender) {

	}

	// Role as a 2PC Coordinator
	private void logTwoPhaseCommitLocallyAsACoordinator() {
		// TODO Auto-generated method stub

	}

	// Role as Paxos Leader
	public void handleTwoPhaseCommitCommit(Transaction t, int shardIdOfSender) {
		// Start Paxos and replicate log
		//Talk your cohorts bruh
		startPaxos("", PaxosType.COMMIT_COMMIT);
		
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
