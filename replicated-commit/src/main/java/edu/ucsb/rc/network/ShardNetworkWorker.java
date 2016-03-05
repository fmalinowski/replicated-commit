package edu.ucsb.rc.network;

import java.net.DatagramPacket;
import java.net.DatagramSocket;

import edu.ucsb.rc.MultiDatacenter;
import edu.ucsb.rc.model.Message;
import edu.ucsb.rc.model.Transaction;

public class ShardNetworkWorker implements Runnable {
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
			multiDatacenter.getCurrentShard().handleTwoPhaseCommitPrepare(transaction, messageFromOtherShard.getShardIdOfSender());
		}
		if (messageFromOtherShard.getMessageType() == Message.MessageType.TWO_PHASE_COMMIT__PREPARE_ACCEPTED) {
			multiDatacenter.getCurrentShard().handleTwoPhaseCommitPrepareAccepted(transaction, messageFromOtherShard.getShardIdOfSender());
		}
		if (messageFromOtherShard.getMessageType() == Message.MessageType.TWO_PHASE_COMMIT__PREPARE_DENIED) {
			multiDatacenter.getCurrentShard().handleTwoPhaseCommitPrepareDenied(transaction, messageFromOtherShard.getShardIdOfSender());
		}
		if (messageFromOtherShard.getMessageType() == Message.MessageType.PAXOS__ACCEPT_REQUEST_ACCEPTED) {
			multiDatacenter.getCurrentShard().handlePaxosAcceptRequestAccepted(transaction, messageFromOtherShard.getShardIdOfSender());
		}
		if (messageFromOtherShard.getMessageType() == Message.MessageType.TWO_PHASE_COMMIT__COMMIT) {
			multiDatacenter.getCurrentShard().handleTwoPhaseCommitCommit(transaction, messageFromOtherShard.getShardIdOfSender());
		}
	}	
}
