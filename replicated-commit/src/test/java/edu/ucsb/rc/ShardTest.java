package edu.ucsb.rc;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import edu.ucsb.rc.network.Message;
import edu.ucsb.rc.network.NetworkHandler;
import edu.ucsb.rc.transactions.Operation;
import edu.ucsb.rc.transactions.Transaction;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Shard.class, Datastore.class})
public class ShardTest {

	@Test
	public void testHandleReadRequestFromClient() {
		Datacenter dc = new Datacenter();
		Shard currentShard = new Shard();
		currentShard.setDatacenter(dc);
		currentShard.setShardID(1);
		
		Datastore datastoreMock = PowerMock.createMock(Datastore.class);
		NetworkHandler networkHandlerMock = PowerMock.createMock(NetworkHandler.class);
		MultiDatacenter.getInstance().setNetworkHandler(networkHandlerMock);
		Message messageMock = PowerMock.createMock(Message.class);
		
		Transaction t = new Transaction();
		t.setServerTransactionId("ip1", 2345, 1);
		Operation op1 = new Operation();
		op1.setKey("a"); // This will be handled by Shard whose id is 1
		Operation op2 = new Operation();
		op2.setKey("b"); // This will be handled by Shard whose id is 2
		Operation op3 = new Operation();
		op3.setKey("d"); // This will be handled by Shard whose id is 1
		ArrayList<Operation> readSet = new ArrayList<Operation>();
		readSet.add(op1);
		readSet.add(op2);
		readSet.add(op3);
		t.setReadSet(readSet);
		
		/* Expect the constructor of Message to be called and return messageMock */
		try {			
			PowerMock.expectNew(Message.class).andReturn(messageMock);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		/* Expect getInstance to be called twice and we return the datastoreMock */
		PowerMock.mockStatic(Datastore.class);
		Datastore.getInstance();
		PowerMock.expectLastCall().andReturn(datastoreMock).times(2);
		
		/* Expect the following calls to be made */
		datastoreMock.read(op1.getKey(), op1.getColumnValues());
		datastoreMock.read(op3.getKey(), op3.getColumnValues());
		messageMock.setTransaction(t);
		messageMock.setMessageType(Message.MessageType.READ_ANSWER);
		networkHandlerMock.sendMessageToClient(t, messageMock);
		
		/* We're done with the configuration of the mocks and expectations. Let's test now */
		PowerMock.replayAll();
		
		/* Let's run the test */
		currentShard.handleReadRequestFromClient(t);
		
		/* Make sure all the expected calls were made */
		PowerMock.verifyAll();
	}
	
	@Test
	public void testHandleReadRequestFromClient__alreadyLocked() {
		Datacenter dc = new Datacenter();
		Shard currentShard = new Shard();
		currentShard.setDatacenter(dc);
		currentShard.setShardID(1);
		
		Datastore datastoreMock = PowerMock.createMock(Datastore.class);
		NetworkHandler networkHandlerMock = PowerMock.createMock(NetworkHandler.class);
		MultiDatacenter.getInstance().setNetworkHandler(networkHandlerMock);
		Message messageMock1 = PowerMock.createMock(Message.class);
		Message messageMock2 = PowerMock.createMock(Message.class);
		
		Transaction t1 = new Transaction();
		t1.setServerTransactionId("ip1", 2345, 1);
		Operation op0 = new Operation();
		op0.setKey("d"); // This will be handled by Shard whose id is 1
		ArrayList<Operation> readSetT1 = new ArrayList<Operation>();
		readSetT1.add(op0);
		t1.setReadSet(readSetT1);
		
		Transaction t2 = new Transaction();
		t2.setServerTransactionId("ip2", 2345, 1);
		Operation op1 = new Operation();
		op1.setKey("a"); // This will be handled by Shard whose id is 1
		Operation op2 = new Operation();
		op2.setKey("b"); // This will be handled by Shard whose id is 2
		Operation op3 = new Operation();
		op3.setKey("d"); // This will be handled by Shard whose id is 1
		ArrayList<Operation> readSet = new ArrayList<Operation>();
		readSet.add(op1);
		readSet.add(op2);
		readSet.add(op3);
		t2.setReadSet(readSet);
		
		/* Expect the constructor of Message to be called and return messageMock */
		try {	
			PowerMock.expectNew(Message.class).andReturn(messageMock1);
			PowerMock.expectNew(Message.class).andReturn(messageMock2);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		/* Expect getInstance to be called twice and we return the datastoreMock */
		PowerMock.mockStatic(Datastore.class);
		Datastore.getInstance();
		PowerMock.expectLastCall().andReturn(datastoreMock).times(2);
		
		/* Expect the following calls to be made */
		datastoreMock.read(op0.getKey(), op0.getColumnValues());
		datastoreMock.read(op1.getKey(), op1.getColumnValues());
		
		messageMock1.setTransaction(t1);
		messageMock1.setMessageType(Message.MessageType.READ_ANSWER);
		networkHandlerMock.sendMessageToClient(t1, messageMock1);
		
		messageMock2.setTransaction(t2);
		messageMock2.setMessageType(Message.MessageType.READ_FAILED);
		networkHandlerMock.sendMessageToClient(t2, messageMock2);
		
		/* We're done with the configuration of the mocks and expectations. Let's test now */
		PowerMock.replayAll();
		
		/* Let's run the test */
		currentShard.handleReadRequestFromClient(t1);
		currentShard.handleReadRequestFromClient(t2);
		
		/* Make sure all the expected calls were made */
		PowerMock.verifyAll();
	}

}
