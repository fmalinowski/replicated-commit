package edu.ucsb.spanner.network;

import java.net.DatagramPacket;
import java.util.logging.Logger;

import edu.ucsb.spanner.MultiDatacenter;
import edu.ucsb.spanner.model.Message;
import edu.ucsb.spanner.model.Transaction;

public class ClientNetworkWorker implements Runnable {
	private final static Logger LOGGER = Logger.getLogger(ClientNetworkWorker.class.getName()); 
	private DatagramPacket packet;
	
	public ClientNetworkWorker(DatagramPacket packet) {
		this.packet = packet;
	}

	public void run() {
		byte[] receivedBytes;
		Message messageFromClient;
		
		receivedBytes = this.packet.getData();
		messageFromClient = Message.deserialize(receivedBytes);
		
		// We handle the message received from the client here  
		MultiDatacenter multiDatacenter = MultiDatacenter.getInstance();
		
		Transaction transaction = messageFromClient.getTransaction();
		setServerTransactionId(this.packet, transaction);
		
		if (messageFromClient.getMessageType() == Message.MessageType.TWO_PHASE_COMMIT__PREPARE) {
			multiDatacenter.getCurrentShard().handleTwoPhaseCommitPrepareFromClient(transaction);
		}
	}
	
	private void setServerTransactionId(DatagramPacket packet, Transaction transaction) {		
		String clientIpAddress = packet.getAddress().getHostAddress();
		int clientPort = packet.getPort();
		long clientTransactionId = transaction.getTransactionIdDefinedByClient();
		
		transaction.setServerTransactionId(clientIpAddress, clientPort, clientTransactionId);
	}
}
