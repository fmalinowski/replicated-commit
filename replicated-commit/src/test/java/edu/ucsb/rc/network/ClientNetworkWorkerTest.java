package edu.ucsb.rc.network;

import static org.junit.Assert.*;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.modules.junit4.PowerMockRunner;

import edu.ucsb.rc.Message;
import edu.ucsb.rc.MultiDatacenter;
import edu.ucsb.rc.Shard;
import edu.ucsb.rc.transactions.Transaction;

public class ClientNetworkWorkerTest {
	
	@Test
	public void testRunReadRequest__handleReadRequestFromClient_is_called() {		
		Transaction transaction = new Transaction();
		transaction.setTransactionIdDefinedByClient(493834134);
		
		Message message = new Message();
		message.setTransaction(transaction);
		message.setMessageType(Message.MessageType.READ_REQUEST);
		byte[] serializedMessage = message.serialize();
		
		/* We mock the currentShard */
		Shard currentShardMock = PowerMock.createMock(Shard.class);
		MultiDatacenter.getInstance().setCurrentShard(currentShardMock);
		
		/* Expect the method handleReadRequestFromClient of the current Shard 
		 * to be called with an instance of Transaction 
		 */
		currentShardMock.handleReadRequestFromClient((Transaction)EasyMock.anyObject());
		
		/* We're done with the configuration of the mocks and expectations. Let's test now */
		PowerMock.replayAll();
		
		/* Let's run the test */
		InetAddress clientInetAddress = null;
		try {
			clientInetAddress = InetAddress.getByName("81.121.123.135");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		DatagramPacket packet = new DatagramPacket(serializedMessage, serializedMessage.length, clientInetAddress, 1345);
		ClientNetworkWorker clientNetworkWorker = new ClientNetworkWorker(packet);
		
		clientNetworkWorker.run();
		
		/* Make sure all the expected calls were made */
		PowerMock.verifyAll();
	}
	
	@Test
	public void testRunReadRequest__correct_transaction_passed_to_shard() {
		Shard currentShard = new Shard();
		MultiDatacenter.getInstance().setCurrentShard(currentShard);
		
		String clientIpAddress = "81.121.123.135";
		int clientPort = 1345;
		long clientTransactionId = 493834134;
		
		Transaction transaction = new Transaction();
		transaction.setTransactionIdDefinedByClient(clientTransactionId);
		transaction.setServerTransactionId(clientIpAddress, clientPort, clientTransactionId);
		
		Message message = new Message();
		message.setTransaction(transaction);
		message.setMessageType(Message.MessageType.READ_REQUEST);
		byte[] serializedMessage = message.serialize();
		
		InetAddress clientInetAddress = null;
		try {
			clientInetAddress = InetAddress.getByName(clientIpAddress);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		DatagramPacket packet = new DatagramPacket(serializedMessage, serializedMessage.length, clientInetAddress, clientPort);
		ClientNetworkWorker clientNetworkWorker = new ClientNetworkWorker(packet);
		
		clientNetworkWorker.run();
		
		Transaction receivedTransaction = currentShard.getTransaction(transaction.getServerTransactionId());
		assertNotNull(receivedTransaction);
		assertEquals(clientTransactionId, receivedTransaction.getTransactionIdDefinedByClient());
	}
	
	@Test
	public void testRunPaxosAcceptRequest__handlePaxosAcceptRequest_is_called() {		
		Transaction transaction = new Transaction();
		transaction.setTransactionIdDefinedByClient(493834134);
		
		Message message = new Message();
		message.setTransaction(transaction);
		message.setMessageType(Message.MessageType.PAXOS__ACCEPT_REQUEST);
		byte[] serializedMessage = message.serialize();
		
		/* We mock the currentShard */
		Shard currentShardMock = PowerMock.createMock(Shard.class);
		MultiDatacenter.getInstance().setCurrentShard(currentShardMock);
		
		/* Expect the method handlePaxosAcceptRequest of the current Shard 
		 * to be called with an instance of Transaction 
		 */
		currentShardMock.handlePaxosAcceptRequest((Transaction)EasyMock.anyObject());
		
		/* We're done with the configuration of the mocks and expectations. Let's test now */
		PowerMock.replayAll();
		
		/* Let's run the test */
		InetAddress clientInetAddress = null;
		try {
			clientInetAddress = InetAddress.getByName("81.121.123.135");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		DatagramPacket packet = new DatagramPacket(serializedMessage, serializedMessage.length, clientInetAddress, 1345);
		ClientNetworkWorker clientNetworkWorker = new ClientNetworkWorker(packet);
		
		clientNetworkWorker.run();
		
		/* Make sure all the expected calls were made */
		PowerMock.verifyAll();
	}
	
	@Test
	public void testRunPaxosAcceptRequest__correct_transaction_passed_to_shard() {
		Shard currentShard = new Shard();
		MultiDatacenter.getInstance().setCurrentShard(currentShard);
		
		String clientIpAddress = "81.121.123.135";
		int clientPort = 1345;
		long clientTransactionId = 493834134;
		
		Transaction transaction = new Transaction();
		transaction.setTransactionIdDefinedByClient(clientTransactionId);
		transaction.setServerTransactionId(clientIpAddress, clientPort, clientTransactionId);
		
		Message message = new Message();
		message.setTransaction(transaction);
		message.setMessageType(Message.MessageType.PAXOS__ACCEPT_REQUEST);
		byte[] serializedMessage = message.serialize();
		
		InetAddress clientInetAddress = null;
		try {
			clientInetAddress = InetAddress.getByName(clientIpAddress);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		DatagramPacket packet = new DatagramPacket(serializedMessage, serializedMessage.length, clientInetAddress, clientPort);
		ClientNetworkWorker clientNetworkWorker = new ClientNetworkWorker(packet);
		
		clientNetworkWorker.run();
		
		Transaction receivedTransaction = currentShard.getTransaction(transaction.getServerTransactionId());
		assertNotNull(receivedTransaction);
		assertEquals(clientTransactionId, receivedTransaction.getTransactionIdDefinedByClient());
	}
}
