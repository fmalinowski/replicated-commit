package edu.ucsb.spanner;

import static edu.ucsb.spanner.Shard.PaxosType.COMMIT_COMMIT;
import static edu.ucsb.spanner.Shard.PaxosType.COMMIT_LOG_REPLICATION;
import static edu.ucsb.spanner.Shard.PaxosType.COMMIT_PREPARE;
import static edu.ucsb.spanner.model.Message.MessageType.PAXOS_PREPARE_FOR_COMMIT_PREPARE;
import static edu.ucsb.spanner.model.Message.MessageType.PAXOS_PREPARE_FOR_FINAL_COMMIT;
import static edu.ucsb.spanner.model.Message.MessageType.PAXOS_PREPARE_FOR_REPLICATE_LOG;
import static edu.ucsb.spanner.model.Message.MessageType.PAXOS_PROMISE_FOR_COMMIT_PREPARE;
import static edu.ucsb.spanner.model.Message.MessageType.PAXOS_PROMISE_FOR_FINAL_COMMIT;
import static edu.ucsb.spanner.model.Message.MessageType.PAXOS_PROMISE_FOR_REPLICATE_LOG;
import static edu.ucsb.spanner.model.Message.MessageType.PAXOS__ACCEPT_REQUEST_ACCEPTED_FOR_COMMIT_PREPARE;
import static edu.ucsb.spanner.model.Message.MessageType.PAXOS__ACCEPT_REQUEST_ACCEPTED_FOR_FINAL_COMMIT;
import static edu.ucsb.spanner.model.Message.MessageType.PAXOS__ACCEPT_REQUEST_ACCEPTED_FOR_REPLICATE_LOG;
import static edu.ucsb.spanner.model.Message.MessageType.PAXOS__ACCEPT_REQUEST_FOR_COMMIT_PREPARE;
import static edu.ucsb.spanner.model.Message.MessageType.PAXOS__ACCEPT_REQUEST_FOR_FINAL_COMMIT;
import static edu.ucsb.spanner.model.Message.MessageType.PAXOS__ACCEPT_REQUEST_FOR_REPLICATE_LOG;
import static edu.ucsb.spanner.model.Message.MessageType.TWO_PHASE_COMMIT_FAILED;
import static edu.ucsb.spanner.model.Message.MessageType.TWO_PHASE_COMMIT__COMMIT;
import static edu.ucsb.spanner.model.Message.MessageType.TWO_PHASE_COMMIT__PREPARE_ACCEPTED;
import static edu.ucsb.spanner.model.Message.MessageType.TWO_PHASE_COMMIT__PREPARE_DENIED;
import static edu.ucsb.spanner.model.Message.MessageType.TWO_PHASE_COMMIT__SUCCESS;

import java.util.ArrayList;
import java.util.logging.Logger;

import edu.ucsb.spanner.locks.LocksManager;
import edu.ucsb.spanner.model.Message;
import edu.ucsb.spanner.model.Message.MessageType;
import edu.ucsb.spanner.model.Operation;
import edu.ucsb.spanner.model.Transaction;
import edu.ucsb.spanner.network.NetworkHandlerInterface;
import edu.ucsb.spanner.network.ShardNetworkWorker;
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

	public enum ResponseType {
		ACCEPTED, DENIED
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

		if (!this.locksManager.acquireExclusiveLocksForTxn(transaction)) {
			// Remove all locks for Txn
			this.locksManager.removeAllLocksForTxn(transaction);
			this.sendMessageToClient(TWO_PHASE_COMMIT_FAILED, transaction);
			return;
		}

		// logTwoPhaseCommitPrepareLocally();

		startPaxos(transaction, COMMIT_PREPARE);
	}

	// Role as Paxos Leader
	private void startPaxos(Transaction transaction, PaxosType type) {

		LOGGER.info("Starting Paxos for " + type + " Transaction SID "
				+ transaction.getServerTransactionId() + " Transation CID "
				+ transaction.getTransactionIdDefinedByClient()
				+ " Current Shard " + this.ipAddress);

		ArrayList<Shard> otherShardsWithId = MultiDatacenter.getInstance()
				.getOtherShardsWithId(this.shardID);

		if (COMMIT_PREPARE == type) {

			paxosManager.startNewPaxosSession(transaction);
			// Send messages to same shard in different datacenters
			//if (otherShardsWithId != null) {
				//for (Shard otherShard : otherShardsWithId) {
					this.sendMessageToOtherShard(this,
							PAXOS_PREPARE_FOR_COMMIT_PREPARE, transaction);
				//}

			//}

		} else if (COMMIT_LOG_REPLICATION == type) {

			paxosManager.startNewPaxosSession(transaction);

			// send msg to other paxos leaders
			//if (otherShardsWithId != null) {
				//for (Shard otherShard : otherShardsWithId) {
					this.sendMessageToOtherShard(this,
							PAXOS_PREPARE_FOR_REPLICATE_LOG, transaction);
				//}

			//}

		} else if (COMMIT_COMMIT == type) {

			paxosManager.startNewPaxosSession(transaction);
			// Send messages to same shard in different datacenters

			//if (otherShardsWithId != null) {
				//for (Shard otherShard : otherShardsWithId) {
					this.sendMessageToOtherShard(this,
							PAXOS_PREPARE_FOR_FINAL_COMMIT, transaction);
				//}

			//}
		}

	}

	// Role as a Shard (sent by leader)
	public void handlePaxosPrepare(Transaction transaction, MessageType type,
			int shardIdOfSender) {

		if (PAXOS_PREPARE_FOR_COMMIT_PREPARE == type) {

			LOGGER.info("Received " + type + " Transaction SID "
					+ transaction.getServerTransactionId() + " Transation CID "
					+ transaction.getTransactionIdDefinedByClient()
					+ " Current Shard " + this.ipAddress);

			// Acquire locks and all
			// Send Message to Leader
			sendMessageToOtherShard(this,
					MessageType.PAXOS_PROMISE_FOR_COMMIT_PREPARE, transaction);
			LOGGER.info("Sending " + PAXOS_PROMISE_FOR_COMMIT_PREPARE
					+ " Transaction SID "
					+ transaction.getServerTransactionId() + " Transation CID "
					+ transaction.getTransactionIdDefinedByClient()
					+ " Current Shard " + this.ipAddress);

		} else if (PAXOS_PREPARE_FOR_FINAL_COMMIT == type) {

			LOGGER.info("Received " + type + " Transaction SID "
					+ transaction.getServerTransactionId() + " Transation CID "
					+ transaction.getTransactionIdDefinedByClient()
					+ " Current Shard " + this.ipAddress);

			// Acquire locks and all
			sendMessageToOtherShard(this,
					MessageType.PAXOS_PROMISE_FOR_FINAL_COMMIT, transaction);
			LOGGER.info("Sending " + PAXOS_PROMISE_FOR_FINAL_COMMIT
					+ " Transaction SID "
					+ transaction.getServerTransactionId() + " Transation CID "
					+ transaction.getTransactionIdDefinedByClient()
					+ " Current Shard " + this.ipAddress);

		} else if (PAXOS_PREPARE_FOR_REPLICATE_LOG == type) {

			LOGGER.info("Received " + type + " Transaction SID "
					+ transaction.getServerTransactionId() + " Transation CID "
					+ transaction.getTransactionIdDefinedByClient()
					+ " Current Shard " + this.ipAddress);

			// Acquire locks and all
			sendMessageToOtherShard(this,
					MessageType.PAXOS_PROMISE_FOR_REPLICATE_LOG, transaction);
			LOGGER.info("Sending " + PAXOS_PROMISE_FOR_REPLICATE_LOG
					+ " Transaction SID "
					+ transaction.getServerTransactionId() + " Transation CID "
					+ transaction.getTransactionIdDefinedByClient()
					+ " Current Shard " + this.ipAddress);
		}

	}

	// Role as Paxos Leader (sent by shards)
	public void handlePaxosPromise(Transaction transaction, MessageType type,
			int shardIdOfSender) {

		ArrayList<Shard> otherShardsWithId = MultiDatacenter.getInstance()
				.getOtherShardsWithId(this.shardID);

		if (PAXOS_PROMISE_FOR_COMMIT_PREPARE == type) {

			LOGGER.info("Received " + type + " Transaction SID "
					+ transaction.getServerTransactionId() + " Transation CID "
					+ transaction.getTransactionIdDefinedByClient()
					+ " Current Shard " + this.ipAddress);

			// send msg to other paxos leaders
			if (otherShardsWithId != null) {
				for (Shard otherShard : otherShardsWithId) {
					this.sendMessageToOtherShard(otherShard,
							PAXOS__ACCEPT_REQUEST_FOR_COMMIT_PREPARE,
							transaction);
				}

			}

		} else if (PAXOS_PROMISE_FOR_FINAL_COMMIT == type) {

			LOGGER.info("Received " + type + " Transaction SID "
					+ transaction.getServerTransactionId() + " Transation CID "
					+ transaction.getTransactionIdDefinedByClient()
					+ " Current Shard " + this.ipAddress);

			// send msg to other paxos leaders
			if (otherShardsWithId != null) {
				for (Shard otherShard : otherShardsWithId) {
					this.sendMessageToOtherShard(otherShard,
							PAXOS__ACCEPT_REQUEST_FOR_FINAL_COMMIT, transaction);
				}

			}

		} else if (PAXOS_PROMISE_FOR_REPLICATE_LOG == type) {

			LOGGER.info("Received " + type + " Transaction SID "
					+ transaction.getServerTransactionId() + " Transation CID "
					+ transaction.getTransactionIdDefinedByClient()
					+ " Current Shard " + this.ipAddress);

			// send msg to other paxos leaders
			if (otherShardsWithId != null) {
				for (Shard otherShard : otherShardsWithId) {
					this.sendMessageToOtherShard(otherShard,
							PAXOS__ACCEPT_REQUEST_FOR_REPLICATE_LOG,
							transaction);
				}

			}

		}

	}

	// Role as Shards (sent by leader)
	public void handlePaxosAccept(Transaction transaction, MessageType type,
			int shardIdOfSender) {

		if (PAXOS__ACCEPT_REQUEST_FOR_COMMIT_PREPARE == type) {

			LOGGER.info("Received " + type + " Transaction SID "
					+ transaction.getServerTransactionId() + " Transation CID "
					+ transaction.getTransactionIdDefinedByClient()
					+ " Current Shard " + this.ipAddress);

			// Acquire locks and all
			sendMessageToOtherShard(
					this,
					MessageType.PAXOS__ACCEPT_REQUEST_ACCEPTED_FOR_COMMIT_PREPARE,
					transaction);

		} else if (PAXOS__ACCEPT_REQUEST_FOR_FINAL_COMMIT == type) {

			LOGGER.info("Received " + type + " Transaction SID "
					+ transaction.getServerTransactionId() + " Transation CID "
					+ transaction.getTransactionIdDefinedByClient()
					+ " Current Shard " + this.ipAddress);

			LOGGER.info("Commiting transaction "
					+ transaction.getTransactionIdDefinedByClient()
					+ " in shard " + this.ipAddress);

			sendMessageToOtherShard(
					this,
					MessageType.PAXOS__ACCEPT_REQUEST_ACCEPTED_FOR_FINAL_COMMIT,
					transaction);

		} else if (PAXOS__ACCEPT_REQUEST_FOR_REPLICATE_LOG == type) {

			LOGGER.info("Received " + type + " Transaction SID "
					+ transaction.getServerTransactionId() + " Transation CID "
					+ transaction.getTransactionIdDefinedByClient()
					+ " Current Shard " + this.ipAddress);

			// Acquire locks and all
			sendMessageToOtherShard(
					this,
					MessageType.PAXOS__ACCEPT_REQUEST_ACCEPTED_FOR_REPLICATE_LOG,
					transaction);
		}

	}

	// Role as Leader (sent by Shards)
	public void handlePaxosAcceptAcceptedOrDenied(Transaction transaction,
			MessageType type, int shardIdOfSender, ResponseType responseType) {

		LOGGER.info("Received " + type + " Transaction SID "
				+ transaction.getServerTransactionId() + " Transation CID "
				+ transaction.getTransactionIdDefinedByClient()
				+ " Current Shard " + this.ipAddress);

		if (PAXOS__ACCEPT_REQUEST_ACCEPTED_FOR_COMMIT_PREPARE == type) {

			boolean acceptStatus = false;
			boolean ticksStatus = false;
			if (ResponseType.ACCEPTED == responseType) {
				acceptStatus = this.paxosManager
						.increaseAcceptAccepted(transaction);
			} else {
				ticksStatus = this.paxosManager.increaseTicks(transaction);
			}

			if (acceptStatus) {

				LOGGER.info("Achieved majority for " + type
						+ " Transaction SID "
						+ transaction.getServerTransactionId()
						+ " Transation CID "
						+ transaction.getTransactionIdDefinedByClient()
						+ " Current Shard " + this.ipAddress);

				sendMessageToOtherShard(this,
						TWO_PHASE_COMMIT__PREPARE_ACCEPTED, transaction);

			} else if (ticksStatus) {

				LOGGER.info("Aborting as there is no majority for " + type
						+ " Transaction SID "
						+ transaction.getServerTransactionId()
						+ " Transation CID "
						+ transaction.getTransactionIdDefinedByClient()
						+ " Current Shard " + this.ipAddress);
				sendMessageToOtherShard(this, TWO_PHASE_COMMIT__PREPARE_DENIED,
						transaction);
			}

		} else if (PAXOS__ACCEPT_REQUEST_ACCEPTED_FOR_FINAL_COMMIT == type) {

			boolean acceptStatus = false;
			boolean ticksStatus = false;
			if (ResponseType.ACCEPTED == responseType) {
				acceptStatus = this.paxosManager
						.increaseAcceptAccepted(transaction);
			} else {
				ticksStatus = this.paxosManager.increaseTicks(transaction);
			}

			if (acceptStatus) {

				LOGGER.info("Releasing all locks cos Achieved majority for "
						+ type + " Transaction SID "
						+ transaction.getServerTransactionId()
						+ " Transation CID "
						+ transaction.getTransactionIdDefinedByClient()
						+ " Current Shard " + this.ipAddress);

				// Release locks
				this.locksManager.removeAllLocksForTxn(transaction);

			} else if (ticksStatus) {

				LOGGER.info("Aborting Final Commit as there is no majority for "
						+ type
						+ " Transaction SID "
						+ transaction.getServerTransactionId()
						+ " Transation CID "
						+ transaction.getTransactionIdDefinedByClient()
						+ " Current Shard " + this.ipAddress);
				// Release locks
				this.locksManager.removeAllLocksForTxn(transaction);
			}

		} else if (PAXOS__ACCEPT_REQUEST_ACCEPTED_FOR_REPLICATE_LOG == type) {

			boolean acceptStatus = false;
			boolean ticksStatus = false;
			if (ResponseType.ACCEPTED == responseType) {
				acceptStatus = this.paxosManager
						.increaseAcceptAccepted(transaction);
			} else {
				ticksStatus = this.paxosManager.increaseTicks(transaction);
			}

			if (acceptStatus) {

				LOGGER.info("Two phase commit successful " + type
						+ " Transaction SID "
						+ transaction.getServerTransactionId()
						+ " Transation CID "
						+ transaction.getTransactionIdDefinedByClient()
						+ " Current Shard " + this.ipAddress);

				sendMessageToClient(TWO_PHASE_COMMIT__SUCCESS, transaction);
				sendMessageToOtherShard(this, TWO_PHASE_COMMIT__COMMIT,
						transaction);

			} else if (ticksStatus) {

				LOGGER.info("Two Phase Commit failed no majority for " + type
						+ " Transaction SID "
						+ transaction.getServerTransactionId()
						+ " Transation CID "
						+ transaction.getTransactionIdDefinedByClient()
						+ " Current Shard " + this.ipAddress);
				sendMessageToClient(TWO_PHASE_COMMIT_FAILED, transaction);

			}
		}
	}

	// Role as a 2PC Coordinator received from leaders
	public void handleTwoPhaseCommitPrepareAccepted(Transaction t,
			int shardIdOfSender) {

		logTwoPhaseCommitLocallyAsACoordinator();
		// send log along with it
		startPaxos(t, COMMIT_LOG_REPLICATION);
	}

	// Role as 2PC Coordinator
	public void handleTwoPhaseCommitPrepareDenied(Transaction t,
			int shardIdOfSender) {
		sendMessageToClient(TWO_PHASE_COMMIT__PREPARE_DENIED, t);

	}

	// Role as a 2PC Coordinator
	private void logTwoPhaseCommitLocallyAsACoordinator() {
		// TODO Auto-generated method stub

	}

	// Role as Paxos Leader
	public void handleTwoPhaseCommitCommit(Transaction t, int shardIdOfSender) {
		startPaxos(t, PaxosType.COMMIT_COMMIT);

	}

	public boolean operationKeyBelongsToCurrentShard(Operation op) {
		return this.datacenter.getShardIdForKey(op.getKey()) == this.shardID;
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

		new Thread(new ShardNetworkWorker(messageForShardSender)).start();
	}

}
