package edu.ucsb.rc.network;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import edu.ucsb.rc.MultiDatacenter;
import edu.ucsb.rc.Shard;
import edu.ucsb.rc.model.Message;
import edu.ucsb.rc.model.Transaction;

public class NetworkHandler implements NetworkHandlerInterface {
	private int shardsPort;
	private ShardsNetworkListener shardsNetworkListener;
	private ClientsNetworkListener clientsNetworkListener;
	private DatagramSocket socketForClients;
	private DatagramSocket socketForShards;
	
	private Lock clientsSocketLock;
	private Lock shardsSocketLock;

	public NetworkHandler(MultiDatacenter multiDatacenter, int clientsPort, int shardsPort) {
		this.shardsPort = shardsPort;
		
		try {
			this.socketForShards = new DatagramSocket(shardsPort);
			this.socketForClients = new DatagramSocket(clientsPort);
			
			this.shardsNetworkListener = new ShardsNetworkListener(socketForShards);
			this.clientsNetworkListener = new ClientsNetworkListener(socketForClients);
			
			this.clientsSocketLock = new ReentrantLock();
			this.shardsSocketLock = new ReentrantLock();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void listenForClients() {
		// Listen for client connections and messages
		this.clientsNetworkListener.start();
	}
	
	public void listenForShards() {
		// Listen for shard connections and messages
		this.shardsNetworkListener.start();
	}
	
	public void sendMessageToShard(Shard shard, Message message) {
		// Send a message to a shard
		InetAddress shardAddress;
		
		try {
			shardAddress = InetAddress.getByName(shard.getIpAddress());
			byte[] bytesToSend = message.serialize();
			
			DatagramPacket sendPacket = new DatagramPacket(bytesToSend, bytesToSend.length, 
					shardAddress, this.shardsPort);
			
			this.socketForShards.send(sendPacket); // It's thread safe
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void sendMessageToClient(Transaction transaction, Message message) {
		// Send a message to a client
		InetAddress clientAddress;
		
		try {
			clientAddress = InetAddress.getByName(transaction.getClientIpAddress());
			byte[] bytesToSend = message.serialize();
		
			DatagramPacket sendPacket = new DatagramPacket(bytesToSend, bytesToSend.length, 
				clientAddress, transaction.getClientPort());
			
			this.socketForClients.send(sendPacket); // It's thread safe
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
