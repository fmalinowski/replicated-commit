package edu.ucsb.rc.network;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import edu.ucsb.rc.TransactionClient;
import edu.ucsb.rc.Message;
import edu.ucsb.rc.MultiDatacenter;
import edu.ucsb.rc.Shard;

public class NetworkHandler {
	private MultiDatacenter multiDatacenter;
	private int clientsPort, shardsPort, portOnClientSide;
	private ShardsNetworkListener shardsNetworkListener;
	private ClientsNetworkListener clientsNetworkListener;
	private DatagramSocket socketForClients;
	private DatagramSocket socketForShards;
	private DatagramSocket socketToSendMessages;

	public NetworkHandler(MultiDatacenter multiDatacenter, int clientsPort, int shardsPort, int portOnClientSide) {
		this.multiDatacenter = multiDatacenter;
		this.clientsPort = clientsPort;
		this.shardsPort = shardsPort;
		this.portOnClientSide = portOnClientSide;
		
		try {
			this.socketForShards = new DatagramSocket(shardsPort);
			this.socketForClients = new DatagramSocket(clientsPort);
			
			this.shardsNetworkListener = new ShardsNetworkListener(socketForShards);
			this.clientsNetworkListener = new ClientsNetworkListener(socketForClients);
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
			
			this.socketForShards.send(sendPacket);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void sendMessageToClient(TransactionClient client, Message message) {
		// Send a message to a client
		InetAddress clientAddress;
		
		try {
			clientAddress = InetAddress.getByName(client.getIpAddress());
			byte[] bytesToSend = message.serialize();
		
			DatagramPacket sendPacket = new DatagramPacket(bytesToSend, bytesToSend.length, 
				clientAddress, this.portOnClientSide);
		
			this.socketForClients.send(sendPacket);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
