package edu.ucsb.rc.network;

import java.net.DatagramPacket;
import java.util.logging.Logger;

import edu.ucsb.rc.App;
import edu.ucsb.rc.MultiDatacenter;
import edu.ucsb.rc.model.Message;
import edu.ucsb.rc.model.Transaction;

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
		LOGGER.info("Got a message");
		messageFromClient = Message.deserialize(receivedBytes);
		LOGGER.info("Got this message type:" + messageFromClient.getMessageType());
		
		// We handle the message received from the client here  
		MultiDatacenter multiDatacenter = MultiDatacenter.getInstance();
		
		Transaction transaction = messageFromClient.getTransaction();
		setServerTransactionId(this.packet, transaction);
		
		if (messageFromClient.getMessageType() == Message.MessageType.READ_REQUEST) {
			LOGGER.info("Received READ_REQUEST from client. clientTransactionID:" + transaction.getTransactionIdDefinedByClient() + " | serverTransactionID:" + transaction.getServerTransactionId());
			multiDatacenter.getCurrentShard().handleReadRequestFromClient(transaction);
		}
		if (messageFromClient.getMessageType() == Message.MessageType.PAXOS__ACCEPT_REQUEST) {
			LOGGER.info("Received PAXOS__ACCEPT_REQUEST from client. clientTransactionID:" + transaction.getTransactionIdDefinedByClient() + " | serverTransactionID:" + transaction.getServerTransactionId());
			multiDatacenter.getCurrentShard().handlePaxosAcceptRequest(transaction);
		}
	}
	
	private void setServerTransactionId(DatagramPacket packet, Transaction transaction) {		
		String clientIpAddress = packet.getAddress().getHostAddress();
		int clientPort = packet.getPort();
		long clientTransactionId = transaction.getTransactionIdDefinedByClient();
		
		transaction.setServerTransactionId(clientIpAddress, clientPort, clientTransactionId);
	}
}
