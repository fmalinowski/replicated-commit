package edu.ucsb.rc.network;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

import edu.ucsb.rc.Message;
import edu.ucsb.rc.MultiDatacenter;
import edu.ucsb.rc.TransactionClient;

public class ClientNetworkWorker implements Runnable {
	private DatagramSocket serverSocket = null;
	private DatagramPacket packet;
	
	public ClientNetworkWorker(DatagramSocket serverSocket, DatagramPacket packet) {
		this.serverSocket = serverSocket;
		this.packet = packet;
	}

	public void run() {
		byte[] receivedBytes;
		Message messageFromClient;
		
		receivedBytes = this.packet.getData();
		messageFromClient = Message.deserialize(receivedBytes);
		
		// We handle the message received from the client here  
		TransactionClient transactionClient = getOrAddTransactionClientToMultiDatacenter(this.packet, messageFromClient);
		MultiDatacenter multiDatacenter = MultiDatacenter.getInstance();
		
		if (messageFromClient.getMessageType() == Message.MessageType.READ_REQUEST) {
			multiDatacenter.getCurrentShard().handleReadRequestFromClient(transactionClient, messageFromClient);
		}
		if (messageFromClient.getMessageType() == Message.MessageType.PAXOS__ACCEPT_REQUEST) {
			multiDatacenter.getCurrentShard().handlePaxosAcceptRequest(transactionClient, messageFromClient);
		}
	}
	
	public TransactionClient getOrAddTransactionClientToMultiDatacenter(DatagramPacket packet, Message message) {
		MultiDatacenter multiDatacenter = MultiDatacenter.getInstance();
		
		String clientIpAddress = packet.getAddress().getHostAddress();
		int clientPort = packet.getPort();
		long transactionIdDefinedByClient = message.getTransactionID();
		
		String serverSideTransactionId = TransactionClient.buildServerSideTransactionID(clientIpAddress, clientPort, transactionIdDefinedByClient);
		
		if (multiDatacenter.containsTransactionClient(serverSideTransactionId)) {
			return multiDatacenter.getTransactionClient(serverSideTransactionId);
		} else {
			TransactionClient tc = new TransactionClient(clientIpAddress, clientPort, transactionIdDefinedByClient);
			multiDatacenter.addTransactionClient(tc);
			return tc;
		}
	}
}
