package edu.ucsb.rc.network;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.logging.Logger;

import edu.ucsb.rc.App;
import edu.ucsb.rc.MultiDatacenter;
import edu.ucsb.rc.model.Message;
import edu.ucsb.rc.model.Transaction;

public class ShardNetworkWorker implements Runnable {
	private final static Logger LOGGER = Logger.getLogger(ShardNetworkWorker.class.getName());
	
	private DatagramSocket serverSocket = null;
	private DatagramPacket packet;
	
	public ShardNetworkWorker(DatagramSocket serverSocket, DatagramPacket packet) {
		this.serverSocket = serverSocket;
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
		
		if (messageFromOtherShard.getMessageType() == Message.MessageType.TWO_PHASE_COMMIT__PREPARE) {
			LOGGER.info("Received TWO_PHASE_COMMIT__PREPARE from shardID:" + messageFromOtherShard.getShardIdOfSender() + " | serverTransactionID:" + transaction.getServerTransactionId());
			multiDatacenter.getCurrentShard().handleTwoPhaseCommitPrepare(transaction, messageFromOtherShard.getShardIdOfSender());
		}
		if (messageFromOtherShard.getMessageType() == Message.MessageType.TWO_PHASE_COMMIT__PREPARE_ACCEPTED) {
			LOGGER.info("Received TWO_PHASE_COMMIT__PREPARE_ACCEPTED from shardID:" + messageFromOtherShard.getShardIdOfSender() + " | serverTransactionID:" + transaction.getServerTransactionId());
			multiDatacenter.getCurrentShard().handleTwoPhaseCommitPrepareAccepted(transaction, messageFromOtherShard.getShardIdOfSender());
		}
		if (messageFromOtherShard.getMessageType() == Message.MessageType.TWO_PHASE_COMMIT__PREPARE_DENIED) {
			LOGGER.info("Received TWO_PHASE_COMMIT__PREPARE_DENIED from shardID:" + messageFromOtherShard.getShardIdOfSender() + " | serverTransactionID:" + transaction.getServerTransactionId());
			multiDatacenter.getCurrentShard().handleTwoPhaseCommitPrepareDenied(transaction, messageFromOtherShard.getShardIdOfSender());
		}
		if (messageFromOtherShard.getMessageType() == Message.MessageType.PAXOS__ACCEPT_REQUEST_ACCEPTED) {
			LOGGER.info("Received PAXOS__ACCEPT_REQUEST_ACCEPTED from shardID:" + messageFromOtherShard.getShardIdOfSender() + " | serverTransactionID:" + transaction.getServerTransactionId());
			multiDatacenter.getCurrentShard().handlePaxosAcceptRequestAccepted(transaction, messageFromOtherShard.getShardIdOfSender());
		}
		if (messageFromOtherShard.getMessageType() == Message.MessageType.TWO_PHASE_COMMIT__COMMIT) {
			LOGGER.info("Received TWO_PHASE_COMMIT__COMMIT from shardID:" + messageFromOtherShard.getShardIdOfSender() + " | serverTransactionID:" + transaction.getServerTransactionId());
			multiDatacenter.getCurrentShard().handleTwoPhaseCommitCommit(transaction, messageFromOtherShard.getShardIdOfSender());
		}
	}	
}
