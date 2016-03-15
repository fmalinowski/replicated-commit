package edu.ucsb.spanner.network;

import java.net.DatagramPacket;
import java.net.DatagramSocket;

public class ShardsNetworkListener extends Thread {
	private static final int BUFFER_SIZE = 65507;
	
	private DatagramSocket serverSocket;
	private boolean shouldStopServer = false;	
	
	public ShardsNetworkListener(DatagramSocket socketForShards) {
		this.serverSocket = socketForShards;
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
				this.serverSocket.receive(packet);
				new Thread(new ShardNetworkWorker(this.serverSocket, packet)).start();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}
