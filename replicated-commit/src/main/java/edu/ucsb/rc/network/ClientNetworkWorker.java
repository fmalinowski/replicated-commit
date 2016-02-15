package edu.ucsb.rc.network;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

import edu.ucsb.rc.Message;

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
	}	
}
