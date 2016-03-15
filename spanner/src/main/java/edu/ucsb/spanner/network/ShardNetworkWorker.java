package edu.ucsb.spanner.network;

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
import static edu.ucsb.spanner.model.Message.MessageType.TWO_PHASE_COMMIT__COMMIT;
import static edu.ucsb.spanner.model.Message.MessageType.TWO_PHASE_COMMIT__PREPARE;
import static edu.ucsb.spanner.model.Message.MessageType.TWO_PHASE_COMMIT__PREPARE_ACCEPTED;
import static edu.ucsb.spanner.model.Message.MessageType.TWO_PHASE_COMMIT__PREPARE_DENIED;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.logging.Logger;

import edu.ucsb.spanner.MultiDatacenter;
import edu.ucsb.spanner.model.Message;
import edu.ucsb.spanner.model.Message.MessageType;
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

		// coming from the Paxos Leader for this kind of shard from different
		// data centers
		MessageType msgType = messageFromOtherShard.getMessageType();
		if (msgType == TWO_PHASE_COMMIT__PREPARE) {
			LOGGER.info("Received TWO_PHASE_COMMIT__PREPARE from shardID:"
					+ messageFromOtherShard.getShardIdOfSender()
					+ " | serverTransactionID:"
					+ transaction.getServerTransactionId());
			multiDatacenter.getCurrentShard()
					.handleTwoPhaseCommitPrepareFromClient(transaction);
		}

		// To be received by 2PC Cooridinator, sent by two Paxos Leaders
		if (msgType == TWO_PHASE_COMMIT__PREPARE_ACCEPTED) {
			LOGGER.info("Received TWO_PHASE_COMMIT__PREPARE_ACCEPTED from shardID:"
					+ messageFromOtherShard.getShardIdOfSender()
					+ " | serverTransactionID:"
					+ transaction.getServerTransactionId());
			multiDatacenter.getCurrentShard()
					.handleTwoPhaseCommitPrepareAccepted(transaction,
							messageFromOtherShard.getShardIdOfSender());
		}

		// To be received by 2PC Coordinator, sent by two Paxos Leaders
		if (msgType == TWO_PHASE_COMMIT__PREPARE_DENIED) {
			LOGGER.info("Received TWO_PHASE_COMMIT__PREPARE_DENIED from shardID:"
					+ messageFromOtherShard.getShardIdOfSender()
					+ " | serverTransactionID:"
					+ transaction.getServerTransactionId());
			multiDatacenter.getCurrentShard()
					.handleTwoPhaseCommitPrepareDenied(transaction,
							messageFromOtherShard.getShardIdOfSender());
		}

		if (msgType == TWO_PHASE_COMMIT__COMMIT) {
			LOGGER.info("Received TWO_PHASE_COMMIT__COMMIT from shardID:"
					+ messageFromOtherShard.getShardIdOfSender()
					+ " | serverTransactionID:"
					+ transaction.getServerTransactionId());
			multiDatacenter.getCurrentShard().handleTwoPhaseCommitCommit(
					transaction, messageFromOtherShard.getShardIdOfSender());
		}

		// Paxos Leaders Receive this from 2PC Coordinnator
		if (msgType == PAXOS_PREPARE_COMMIT_PREPARE
				|| msgType == PAXOS_PREPARE_COMMIT
				|| msgType == PAXOS_PREPARE_REPLICATE_LOG) {

			LOGGER.info("Received " + msgType + " from shardID:"
					+ messageFromOtherShard.getShardIdOfSender()
					+ " | serverTransactionID:"
					+ transaction.getServerTransactionId());
			multiDatacenter.getCurrentShard().handlePaxosPrepare(transaction,
					msgType, messageFromOtherShard.getShardIdOfSender());
		}

		if (msgType == PAXOS_PROMISE_COMMIT_PREPARE
				|| msgType == PAXOS_PROMISE_COMMIT
				|| msgType == PAXOS_PROMISE_REPLICATE_LOG) {
			LOGGER.info("Received " + msgType + " from shardID:"
					+ messageFromOtherShard.getShardIdOfSender()
					+ " | serverTransactionID:"
					+ transaction.getServerTransactionId());
			multiDatacenter.getCurrentShard().handlePaxosPromise(transaction,

			msgType, messageFromOtherShard.getShardIdOfSender());
		}

		if (msgType == PAXOS__ACCEPT_REQUEST_COMMIT_PREPARE
				|| msgType == PAXOS__ACCEPT_REQUEST_COMMIT
				|| msgType == PAXOS__ACCEPT_REQUEST_REPLICATE_LOG) {
			LOGGER.info("Received " + msgType + " from shardID:"
					+ messageFromOtherShard.getShardIdOfSender()
					+ " | serverTransactionID:"
					+ transaction.getServerTransactionId());
			multiDatacenter.getCurrentShard().handlePaxosAccept(transaction,
					msgType, messageFromOtherShard.getShardIdOfSender());
		}

		if (msgType == PAXOS__ACCEPT_REQUEST_ACCEPTED_COMMIT_PREPARE
				|| msgType == PAXOS__ACCEPT_REQUEST_ACCEPTED_COMMIT
				|| msgType == PAXOS__ACCEPT_REQUEST_ACCEPTED_REPLICATE_LOG) {
			LOGGER.info("Received " + msgType + " from shardID:"
					+ messageFromOtherShard.getShardIdOfSender()
					+ " | serverTransactionID:"
					+ transaction.getServerTransactionId());
			multiDatacenter.getCurrentShard().handlePaxosAcceptAccepted(
					transaction, msgType,
					messageFromOtherShard.getShardIdOfSender());
		}

		if (msgType == PAXOS__ACCEPT_REQUEST_DENIED_COMMIT_PREPARE
				|| msgType == PAXOS__ACCEPT_REQUEST_DENIED_COMMIT
				|| msgType == PAXOS__ACCEPT_REQUEST_DENIED_REPLICATE_LOG) {
			LOGGER.info("Received " + msgType + " from shardID:"
					+ messageFromOtherShard.getShardIdOfSender()
					+ " | serverTransactionID:"
					+ transaction.getServerTransactionId());
			multiDatacenter.getCurrentShard().handlePaxosAcceptRejected(
					transaction, msgType,
					messageFromOtherShard.getShardIdOfSender());
		}

	}
}
