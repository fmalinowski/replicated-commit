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

import edu.ucsb.rc.model.Message;
import edu.ucsb.rc.model.Operation;
import edu.ucsb.rc.model.Transaction;
import edu.ucsb.rc.network.NetworkHandlerInterface;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Shard.class, Datastore.class})
public class ScenariiTest {
	
	class NetworkHandlerMock implements NetworkHandlerInterface {
		public int successReadCounts = 0;
		public int clientPaxosAcceptRequestAcceptedCount = 0;
		
		public int tpcPrepareCount = 0;
		public int tpcPrepareAcceptedCount = 0;
		public int tpcPrepareDeniedCount = 0;
		public int tpcCommitCount = 0;
		public int paxosAcceptRequestAcceptedCount = 0;
		public int paxosAcceptRequestDeniedCount = 0;

		public void sendMessageToShard(Shard shard, Message message) {
			if (message.getMessageType() == Message.MessageType.TWO_PHASE_COMMIT__PREPARE) {
				tpcPrepareCount++;
			}
			if (message.getMessageType() == Message.MessageType.TWO_PHASE_COMMIT__PREPARE_ACCEPTED) {
				tpcPrepareAcceptedCount++;
			}
			if (message.getMessageType() == Message.MessageType.TWO_PHASE_COMMIT__PREPARE_DENIED) {
				tpcPrepareDeniedCount++;
			}
			if (message.getMessageType() == Message.MessageType.TWO_PHASE_COMMIT__COMMIT) {
				tpcCommitCount++;
			}
			if (message.getMessageType() == Message.MessageType.PAXOS__ACCEPT_REQUEST_ACCEPTED) {
				paxosAcceptRequestAcceptedCount++;
			}
			if (message.getMessageType() == Message.MessageType.PAXOS__ACCEPT_REQUEST_DENIED) {
				paxosAcceptRequestDeniedCount++;
			}
		}

		public void sendMessageToClient(Transaction transaction, Message message) {
			if (message.getMessageType() == Message.MessageType.READ_ANSWER) {
				successReadCounts++;
			}
			
			if (message.getMessageType() == Message.MessageType.PAXOS__ACCEPT_REQUEST_ACCEPTED) {
				clientPaxosAcceptRequestAcceptedCount++;
			}
		}
		
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
		MultiDatacenter.getInstance().removeAllDatacenters();
	}

	
	/*
	 * Set of keys that are handled by shard of id 2 (datacenter of 3 shards)
	 * b, e, h, k, n, q, ...
	 */
	@Test
	public void testSingleClient() {
		ConfigReader configReader;
		MultiDatacenter multiDatacenter;
		Shard currentShard;
		
		/* SETUP FOR THE TEST */
		configReader = new ConfigReader("config-unit-test.properties");
		
		multiDatacenter = configReader.initializeMultiDatacenter();
		currentShard = multiDatacenter.getCurrentShard();
		
		NetworkHandlerMock networkHandlerMock = new NetworkHandlerMock(); 
		multiDatacenter.setNetworkHandler(networkHandlerMock);
		multiDatacenter.initializeShards();
		
		Transaction tForRead1 = new Transaction();
		tForRead1.setServerTransactionId("whatever", 12345, 1);
		ArrayList<Operation> readList1 = new ArrayList<Operation>();
		tForRead1.setReadSet(readList1);
		
		Transaction tForRead2 = new Transaction();
		tForRead2.setServerTransactionId("whatever", 12345, 1);
		ArrayList<Operation> readList2 = new ArrayList<Operation>();
		tForRead2.setReadSet(readList2);
		
		Transaction t = new Transaction();
		t.setServerTransactionId("whatever", 12345, 1);
		ArrayList<Operation> readList = new ArrayList<Operation>();
		ArrayList<Operation> writeList = new ArrayList<Operation>();
		t.setReadSet(readList);
		t.setWriteSet(writeList);
		
		Operation read1 = new Operation();
		HashMap<String, String> read1ColumnValues = new HashMap<String, String>();
		read1.setType(Operation.Type.READ);
		read1.setKey("b");
		read1.setColumnValues(read1ColumnValues);
		readList1.add(read1);
		readList.add(read1);
		
		
		Operation read2 = new Operation();
		HashMap<String, String> read2ColumnValues = new HashMap<String, String>();
		read2.setType(Operation.Type.READ);
		read2.setKey("e");
		read2.setColumnValues(read2ColumnValues);
		readList2.add(read2);
		readList.add(read2);
		
		Operation write1 = new Operation();
		HashMap<String, String> write1ColumnValues = new HashMap<String, String>();
		write1.setType(Operation.Type.WRITE);
		write1.setKey("h");
		write1.setColumnValues(write1ColumnValues);
		writeList.add(write1);
		
		Operation write2 = new Operation();
		HashMap<String, String> write2ColumnValues = new HashMap<String, String>();
		write2.setType(Operation.Type.WRITE);
		write2.setKey("b");
		write2.setColumnValues(write2ColumnValues);
		writeList.add(write2);
		
		/* Expect getInstance to be called 3 times and we return the datastoreMock */
		/* 2 times for the reads and one for the write (commit) */
		Datastore datastoreMock = PowerMock.createMock(Datastore.class);
		PowerMock.mockStatic(Datastore.class);
		Datastore.getInstance();
		PowerMock.expectLastCall().andReturn(datastoreMock).times(3);
		
		datastoreMock.read(read1.getKey(), read1.getColumnValues());
		PowerMock.expectLastCall().andReturn(1);
		
		datastoreMock.read(read2.getKey(), read2.getColumnValues());
		PowerMock.expectLastCall().andReturn(2);
		
		datastoreMock.write("h", write1ColumnValues);
		
		datastoreMock.write("b", write2ColumnValues);
		
		
		/* We're done with the configuration of the mocks and expectations. Let's test now */
		PowerMock.replayAll();
		
		/* Let's run the test */
		
		currentShard.handleReadRequestFromClient(tForRead1);
		assertEquals(1, networkHandlerMock.successReadCounts);
		
		currentShard.handleReadRequestFromClient(tForRead2);
		assertEquals(2, networkHandlerMock.successReadCounts);
		
		currentShard.handlePaxosAcceptRequest(t);
		assertEquals(3, networkHandlerMock.tpcPrepareCount);
		
		currentShard.handleTwoPhaseCommitPrepare(t, 2); // Coming from current shard itself
		assertEquals(0, networkHandlerMock.tpcPrepareDeniedCount);
		assertEquals(1, networkHandlerMock.tpcPrepareAcceptedCount);
		assertEquals(0, networkHandlerMock.clientPaxosAcceptRequestAcceptedCount);
		
		currentShard.handleTwoPhaseCommitPrepareAccepted(t, 2); // Coming from current shard itself
		assertEquals(0, networkHandlerMock.paxosAcceptRequestAcceptedCount);
		assertEquals(0, networkHandlerMock.clientPaxosAcceptRequestAcceptedCount);
		
		currentShard.handleTwoPhaseCommitPrepareAccepted(t, 0); // Coming from other shard in same DC
		assertEquals(0, networkHandlerMock.paxosAcceptRequestAcceptedCount);
		assertEquals(0, networkHandlerMock.clientPaxosAcceptRequestAcceptedCount);
		
		currentShard.handleTwoPhaseCommitPrepareAccepted(t, 1); // Coming from other shard in same DC
		assertEquals(2, networkHandlerMock.paxosAcceptRequestAcceptedCount);
		assertEquals(1, networkHandlerMock.clientPaxosAcceptRequestAcceptedCount);
		
		currentShard.handlePaxosAcceptRequestAccepted(t, 2); // Coming from same shardID in other DC
		assertEquals(3, networkHandlerMock.tpcCommitCount);
		
		currentShard.handlePaxosAcceptRequestAccepted(t, 2); // Coming from same shardID in other DC
		assertEquals(3, networkHandlerMock.tpcCommitCount);
		
		currentShard.handleTwoPhaseCommitCommit(t, 2);
		
		/* Make sure all the expected calls were made */
		PowerMock.verifyAll();
	}

}
