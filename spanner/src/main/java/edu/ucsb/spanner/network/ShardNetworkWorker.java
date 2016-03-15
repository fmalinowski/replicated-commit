package edu.ucsb.spanner.network;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.logging.Logger;

import edu.ucsb.spanner.MultiDatacenter;
import edu.ucsb.spanner.model.Message;
import edu.ucsb.spanner.model.Transaction;

public class ShardNetworkWorker implements Runnable {
	private final static Logger LOGGER = Logger
			.getLogger(ShardNetworkWorker.class.getName());

	private DatagramPacket packet;

	public ShardNetworkWorker(DatagramSocket serverSocket, DatagramPacket packet) {
		this.packet = packet;
	}

	public void run() {
		byte[] receivedBytes;
		Message messageFromOtherShard;

		receivedBytes = this.packet.getData();
		messageFromOtherShard = Message.deserialize(receivedBytes);
		// We handle the message received from the other shard here

		MultiDatacenter multiDatacenter = MultiDatacenter.getInstance();

		Transaction transaction = messageFromOtherShard.getTransaction();
		
		//coming from the Paxos Leader for this kind of shard from different data centers
		if (messageFromOtherShard.getMessageType() == Message.MessageType.TWO_PHASE_COMMIT__PREPARE) {
			LOGGER.info("Received TWO_PHASE_COMMIT__PREPARE from shardID:"
					+ messageFromOtherShard.getShardIdOfSender()
					+ " | serverTransactionID:"
					+ transaction.getServerTransactionId());
			multiDatacenter.getCurrentShard()
					.handleTwoPhaseCommitPrepareFromPaxosLeader();
		}

		
		//To be received by 2PC Cooridinator, sent by two Paxos Leaders
		if (messageFromOtherShard.getMessageType() == Message.MessageType.TWO_PHASE_COMMIT__PREPARE_ACCEPTED) {
			LOGGER.info("Received TWO_PHASE_COMMIT__PREPARE_ACCEPTED from shardID:"
					+ messageFromOtherShard.getShardIdOfSender()
					+ " | serverTransactionID:"
					+ transaction.getServerTransactionId());
			multiDatacenter.getCurrentShard()
					.handleTwoPhaseCommitPrepareAccepted(transaction,
							messageFromOtherShard.getShardIdOfSender());
		}

		//To be received by 2PC Cooridinator, sent by two Paxos Leaders
		if (messageFromOtherShard.getMessageType() == Message.MessageType.TWO_PHASE_COMMIT__PREPARE_DENIED) {
			LOGGER.info("Received TWO_PHASE_COMMIT__PREPARE_DENIED from shardID:"
					+ messageFromOtherShard.getShardIdOfSender()
					+ " | serverTransactionID:"
					+ transaction.getServerTransactionId());
			multiDatacenter.getCurrentShard()
					.handleTwoPhaseCommitPrepareDenied(transaction,
							messageFromOtherShard.getShardIdOfSender());
		}
		
		if (messageFromOtherShard.getMessageType() == Message.MessageType.TWO_PHASE_COMMIT__COMMIT) {
			LOGGER.info("Received TWO_PHASE_COMMIT__COMMIT from shardID:"
					+ messageFromOtherShard.getShardIdOfSender()
					+ " | serverTransactionID:"
					+ transaction.getServerTransactionId());
			multiDatacenter.getCurrentShard().handleTwoPhaseCommitCommit(
					transaction, messageFromOtherShard.getShardIdOfSender());
		}
		
		//Paxos Request -Paxos Leader for Commit
		//Paxos Accept - 2PC
		//Paxos Reject - 2PC
		
		//Paxos Leaders Receive this from 2PC Coordinnator 
		if (messageFromOtherShard.getMessageType() == Message.MessageType.PAXOS_PREPARE) {
			LOGGER.info("Received PAXOS_PREPARE from shardID:"
					+ messageFromOtherShard.getShardIdOfSender()
					+ " | serverTransactionID:"
					+ transaction.getServerTransactionId());
			multiDatacenter.getCurrentShard().handlePaxosPrepare(
					transaction, messageFromOtherShard.getShardIdOfSender());
		}
		
		if (messageFromOtherShard.getMessageType() == Message.MessageType.PAXOS_PROMISE) {
			LOGGER.info("Received PAXOS_PROMISE from shardID:"
					+ messageFromOtherShard.getShardIdOfSender()
					+ " | serverTransactionID:"
					+ transaction.getServerTransactionId());
			multiDatacenter.getCurrentShard().handlePaxosPromise(
					transaction, messageFromOtherShard.getShardIdOfSender());
		}
		
		
		if (messageFromOtherShard.getMessageType() == Message.MessageType.PAXOS__ACCEPT_REQUEST) {
			LOGGER.info("Received PAXOS__ACCEPT_REQUEST from shardID:"
					+ messageFromOtherShard.getShardIdOfSender()
					+ " | serverTransactionID:"
					+ transaction.getServerTransactionId());
			multiDatacenter.getCurrentShard().handlePaxosAccept(
					transaction, messageFromOtherShard.getShardIdOfSender());
		}
		
		
		if (messageFromOtherShard.getMessageType() == Message.MessageType.PAXOS__ACCEPT_REQUEST_ACCEPTED) {
			LOGGER.info("Received PAXOS__ACCEPT_REQUEST_ACCEPTED from shardID:"
					+ messageFromOtherShard.getShardIdOfSender()
					+ " | serverTransactionID:"
					+ transaction.getServerTransactionId());
			multiDatacenter.getCurrentShard().handlePaxosAcceptAccepted(
					transaction, messageFromOtherShard.getShardIdOfSender());
		}
		
		if (messageFromOtherShard.getMessageType() == Message.MessageType.PAXOS__ACCEPT_REQUEST_DENIED) {
			LOGGER.info("Received PAXOS__ACCEPT_REQUEST_DENIED from shardID:"
					+ messageFromOtherShard.getShardIdOfSender()
					+ " | serverTransactionID:"
					+ transaction.getServerTransactionId());
			multiDatacenter.getCurrentShard().handlePaxosAcceptRejected(
					transaction, messageFromOtherShard.getShardIdOfSender());
		}
		

	}
}
