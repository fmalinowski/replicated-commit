package edu.ucsb.rc.network;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.logging.Logger;

public class ClientsNetworkListener extends Thread {
	private static final int BUFFER_SIZE = 65507;
	
	private final static Logger LOGGER = Logger.getLogger(ClientsNetworkListener.class.getName());
	
	private DatagramSocket serverSocket;
	private boolean shouldStopServer = false;	
	
	public ClientsNetworkListener(DatagramSocket socketForClients) {
		this.serverSocket = socketForClients;
	}
	
	public void haltServer() {
		this.shouldStopServer = true;
	}
	
	public void run() {
		while (true) {
			DatagramPacket packet;
			byte[] buffer;
			
			if (this.shouldStopServer) {
				return;
			}
			
			buffer = new byte[BUFFER_SIZE];
			packet = new DatagramPacket(buffer, buffer.length);

			try {
				LOGGER.info("Waiting for messages from clients");
				this.serverSocket.receive(packet);
				new Thread(new ClientNetworkWorker(packet)).start();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}

