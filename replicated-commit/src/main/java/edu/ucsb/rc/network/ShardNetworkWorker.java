package edu.ucsb.rc.network;

import java.net.DatagramPacket;
import java.net.DatagramSocket;

import edu.ucsb.rc.MultiDatacenter;
import edu.ucsb.rc.transactions.Transaction;

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
	}	
}
