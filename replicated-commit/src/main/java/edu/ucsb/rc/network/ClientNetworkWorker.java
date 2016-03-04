package edu.ucsb.rc.network;

import java.net.DatagramPacket;
import java.io.*;

import edu.ucsb.rc.MultiDatacenter;
import edu.ucsb.rc.transactions.Transaction;

public class ClientNetworkWorker implements Runnable {
	private DatagramPacket packet;
	
	public ClientNetworkWorker(DatagramPacket packet) {
		this.packet = packet;
	}
 
	/*public void run() {
		byte[] receivedBytes;
		Message messageFromClient;
		
		receivedBytes = this.packet.getData();
		messageFromClient = Message.deserialize(receivedBytes);
		
		// We handle the message received from the client here  
		MultiDatacenter multiDatacenter = MultiDatacenter.getInstance();
		
		Transaction transaction = messageFromClient.getTransaction();
		setServerTransactionId(this.packet, transaction);
		
		if (messageFromClient.getMessageType() == Message.MessageType.READ_REQUEST) {
			multiDatacenter.getCurrentShard().handleReadRequestFromClient(transaction);
		}
		if (messageFromClient.getMessageType() == Message.MessageType.PAXOS__ACCEPT_REQUEST) {
			multiDatacenter.getCurrentShard().handlePaxosAcceptRequest(transaction);
		}
	}*/


	

		public void run() {
		byte[] receivedBytes;
		Message messageFromClient;

		BufferedWriter bw;
		try {
			bw = new BufferedWriter(new FileWriter("output.txt", true));
			bw.write("............RUN METHOD CALLED BY THE CLIENT...........");

			receivedBytes = this.packet.getData();
			messageFromClient = Message.deserialize(receivedBytes);

	        bw.write("----------------------"+messageFromClient.toString()+"--------------------");
	        bw.close();
		} catch (IOException e) {
			
			e.printStackTrace();
		}		
	}

	
	private void setServerTransactionId(DatagramPacket packet, Transaction transaction) {		
		String clientIpAddress = packet.getAddress().getHostAddress();
		int clientPort = packet.getPort();
		long clientTransactionId = transaction.getTransactionIdDefinedByClient();
		
		transaction.setServerTransactionId(clientIpAddress, clientPort, clientTransactionId);
	}
}
