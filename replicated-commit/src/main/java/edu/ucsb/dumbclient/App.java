package edu.ucsb.dumbclient;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;

import edu.ucsb.rc.model.Message;
import edu.ucsb.rc.model.Operation;
import edu.ucsb.rc.model.Transaction;

public class App {
	
	public static void main(String[] args) {
		String serverIpAddress = "128.111.43.43";
		int portNumber = 50002;
		
		DatagramSocket socket = null;
		InetAddress serverAddress = null;
		
		try {
			serverAddress = InetAddress.getByName(serverIpAddress);
			socket = new DatagramSocket();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Message message = new Message();
		message.setMessageType(Message.MessageType.READ_REQUEST);
		
		Transaction t = new Transaction();
		Operation readOp = new Operation();
		readOp.setKey("a");
		readOp.setColumnValues(new HashMap<String, String>());
		ArrayList<Operation> readSet = new ArrayList<Operation>();
		readSet.add(readOp);
		t.setReadSet(readSet);
		t.setTransactionIdDefinedByClient(5);
		message.setTransaction(t);
		
		byte[] bytesToSend = message.serialize();

		DatagramPacket sendPacket = new DatagramPacket(bytesToSend,
				bytesToSend.length, serverAddress, portNumber);

		System.out.println("Sending Read request to server");
		
		try {
			socket.send(sendPacket);
			System.out.println("Sent Read request to server");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		int bufferSize = 65507;
		byte[] buffer = new byte[bufferSize];
		DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
		
		System.out.println("Waiting for answer from server");
		
		try {
			socket.receive(packet);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		byte[] receivedBytes = packet.getData();
		Message messageFromServer = Message.deserialize(receivedBytes);
		
		System.out.println("Received message from server");
		if (messageFromServer.getMessageType() == Message.MessageType.READ_ANSWER) {
			System.out.println("Received READ_ANSWER");
		}
		if (messageFromServer.getMessageType() == Message.MessageType.READ_FAILED) {
			System.out.println("Received READ_FAILED");
		}
	}
}
