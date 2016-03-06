//package edu.ucsb.rc;
//
//import static org.junit.Assert.*;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.powermock.api.easymock.PowerMock;
//import org.powermock.core.classloader.annotations.PrepareForTest;
//import org.powermock.modules.junit4.PowerMockRunner;
//
//import edu.ucsb.rc.model.Message;
//import edu.ucsb.rc.model.Operation;
//import edu.ucsb.rc.model.Transaction;
//import edu.ucsb.rc.network.NetworkHandler;
//
//@RunWith(PowerMockRunner.class)
//@PrepareForTest({Shard.class, Datastore.class})
//public class ShardTest {
//
//	@Test
//	public void testHandleReadRequestFromClient() throws Exception {
//		Datacenter dc = new Datacenter();
//		Shard currentShard = new Shard();
//		currentShard.setDatacenter(dc);
//		currentShard.setShardID(1);
//		
//		Shard otherShard0 = new Shard();
//		Shard otherShard2 = new Shard();
//		otherShard0.setShardID(0);
//		otherShard2.setShardID(2);
//		dc.addShard(otherShard0);
//		dc.addShard(otherShard2);
//		
//		Datastore datastoreMock = PowerMock.createMock(Datastore.class);
//		NetworkHandler networkHandlerMock = PowerMock.createMock(NetworkHandler.class);
//		MultiDatacenter.getInstance().setNetworkHandler(networkHandlerMock);
//		Message messageMock = PowerMock.createMock(Message.class);
//		
//		Transaction t = new Transaction();
//		t.setServerTransactionId("ip1", 2345, 1);
//		Operation op1 = new Operation();
//		op1.setKey("a"); // This will be handled by Shard whose id is 1
//		Operation op2 = new Operation();
//		op2.setKey("b"); // This will be handled by Shard whose id is 2
//		Operation op3 = new Operation();
//		op3.setKey("d"); // This will be handled by Shard whose id is 1
//		ArrayList<Operation> readSet = new ArrayList<Operation>();
//		readSet.add(op1);
//		readSet.add(op2);
//		readSet.add(op3);
//		t.setReadSet(readSet);
//		
//		/* Expect the constructor of Message to be called and return messageMock */
//		try {			
//			PowerMock.expectNew(Message.class).andReturn(messageMock);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		
//		/* Expect getInstance to be called twice and we return the datastoreMock */
//		PowerMock.mockStatic(Datastore.class);
//		Datastore.getInstance();
//		PowerMock.expectLastCall().andReturn(datastoreMock).times(2);
//		
//		/* Expect the following calls to be made */
//		long mockTimestampForOp1 = 1000000;
//		long mockTimestampForOp3 = 2000000;
//		datastoreMock.read(op1.getKey(), op1.getColumnValues());
//		PowerMock.expectLastCall().andReturn(mockTimestampForOp1);
//		datastoreMock.read(op3.getKey(), op3.getColumnValues());
//		PowerMock.expectLastCall().andReturn(mockTimestampForOp3);
//		messageMock.setTransaction(t);
//		messageMock.setMessageType(Message.MessageType.READ_ANSWER);
//		networkHandlerMock.sendMessageToClient(t, messageMock);
//		
//		/* We're done with the configuration of the mocks and expectations. Let's test now */
//		PowerMock.replayAll();
//		
//		/* Let's run the test */
//		currentShard.handleReadRequestFromClient(t);
//		
//		/* We make sure that the read handler set the timestamp of last update for the read operations */
//		assertEquals(mockTimestampForOp1, op1.getTimestamp());
//		assertEquals(mockTimestampForOp3, op3.getTimestamp());
//		
//		/* Make sure all the expected calls were made */
//		PowerMock.verifyAll();
//	}
//	
//	@Test
//	public void testHandleReadRequestFromClient__alreadyLocked() throws Exception {
//		Datacenter dc = new Datacenter();
//		Shard currentShard = new Shard();
//		currentShard.setDatacenter(dc);
//		currentShard.setShardID(1);
//		
//		Shard otherShard0 = new Shard();
//		Shard otherShard2 = new Shard();
//		otherShard0.setShardID(0);
//		otherShard2.setShardID(2);
//		dc.addShard(otherShard0);
//		dc.addShard(otherShard2);
//		
//		Datastore datastoreMock = PowerMock.createMock(Datastore.class);
//		NetworkHandler networkHandlerMock = PowerMock.createMock(NetworkHandler.class);
//		MultiDatacenter.getInstance().setNetworkHandler(networkHandlerMock);
//		Message messageMock1 = PowerMock.createMock(Message.class);
//		Message messageMock2 = PowerMock.createMock(Message.class);
//		
//		Transaction t1 = new Transaction();
//		t1.setServerTransactionId("ip1", 2345, 1);
//		Operation op0 = new Operation();
//		op0.setKey("d"); // This will be handled by Shard whose id is 1
//		op0.setType(Operation.Type.READ);
//		ArrayList<Operation> readSetT1 = new ArrayList<Operation>();
//		readSetT1.add(op0);
//		t1.setReadSet(readSetT1);
//		
//		Transaction t2 = new Transaction();
//		t2.setServerTransactionId("ip2", 2345, 1);
//		Operation op1 = new Operation();
//		op1.setKey("a"); // This will be handled by Shard whose id is 1
//		op1.setType(Operation.Type.READ);
//		Operation op2 = new Operation();
//		op2.setKey("b"); // This will be handled by Shard whose id is 2
//		op2.setType(Operation.Type.READ);
//		Operation op3 = new Operation();
//		op3.setKey("d"); // This will be handled by Shard whose id is 1
//		op3.setType(Operation.Type.READ);
//		ArrayList<Operation> readSet = new ArrayList<Operation>();
//		readSet.add(op1);
//		readSet.add(op2);
//		readSet.add(op3);
//		t2.setReadSet(readSet);
//		
//		/* Expect the constructor of Message to be called and return messageMock */
//		try {	
//			PowerMock.expectNew(Message.class).andReturn(messageMock1);
//			PowerMock.expectNew(Message.class).andReturn(messageMock2);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		
//		/* Expect getInstance to be called twice and we return the datastoreMock */
//		PowerMock.mockStatic(Datastore.class);
//		Datastore.getInstance();
//		PowerMock.expectLastCall().andReturn(datastoreMock).times(2);
//		
//		/* Expect the following calls to be made */
//		long mockTimestampForOp0 = 1000000;
//		long mockTimestampForOp1 = 2000000; 
//		datastoreMock.read(op0.getKey(), op0.getColumnValues());
//		PowerMock.expectLastCall().andReturn(mockTimestampForOp0);
//		datastoreMock.read(op1.getKey(), op1.getColumnValues());
//		PowerMock.expectLastCall().andReturn(mockTimestampForOp1);
//		
//		messageMock1.setTransaction(t1);
//		messageMock1.setMessageType(Message.MessageType.READ_ANSWER);
//		networkHandlerMock.sendMessageToClient(t1, messageMock1);
//		
//		messageMock2.setTransaction(t2);
//		messageMock2.setMessageType(Message.MessageType.READ_FAILED);
//		networkHandlerMock.sendMessageToClient(t2, messageMock2);
//		
//		/* We're done with the configuration of the mocks and expectations. Let's test now */
//		PowerMock.replayAll();
//		
//		/* Let's run the test */
//		currentShard.handleReadRequestFromClient(t1);
//		currentShard.handleReadRequestFromClient(t2);
//		
//		/* We make sure that the read handler set the timestamp of last update for the read operations */
//		assertEquals(mockTimestampForOp0, op0.getTimestamp());
//		assertEquals(mockTimestampForOp1, op1.getTimestamp());
//		
//		/* Make sure all the expected calls were made */
//		PowerMock.verifyAll();
//	}
//
//}
