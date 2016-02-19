package edu.ucsb.rc;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.ucsb.rc.transactions.Operation;
import edu.ucsb.rc.transactions.Transaction;

public class MessageTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testSerializeAndDeserialize() {
		Message message1 = new Message();
		
		Transaction t1 = new Transaction();
		t1.setTransactionIdDefinedByClient(4567891);
		t1.setServerTransactionId("ip1", 8967, 4567891);
		
		Operation op1 = new Operation();
		op1.setType(Operation.Type.WRITE);
		op1.setKey("key1");
		HashMap<String, String> columnValues1 = new HashMap<String, String>();
		columnValues1.put("column1", "value1");
		columnValues1.put("column2", "value2");
		op1.setColumnValues(columnValues1);
		
		Operation op2 = new Operation();
		op2.setType(Operation.Type.READ);
		op2.setKey("key2");
		HashMap<String, String> columnValues2 = new HashMap<String, String>();
		columnValues1.put("column3", "value3");
		columnValues1.put("column4", "value4");
		op2.setColumnValues(columnValues2);
		
		ArrayList<Operation> writeSet = new ArrayList<Operation>();
		writeSet.add(op1);
		
		ArrayList<Operation> readSet = new ArrayList<Operation>();
		readSet.add(op2);
		
		t1.setReadSet(readSet);
		t1.setWriteSet(writeSet);
		
		message1.setMessageType(Message.MessageType.READ_REQUEST);
		message1.setTransaction(t1);
		
		/*
		 * We test the serialization and deserialization below
		 */
		
		byte[] serializedMessage = message1.serialize();
		
		Message deserializedMessage = Message.deserialize(serializedMessage);
		Transaction deserializedTransaction = deserializedMessage.getTransaction();
		
		assertEquals(Message.MessageType.READ_REQUEST, deserializedMessage.getMessageType());
		assertNotNull(deserializedTransaction);
		
		ArrayList<Operation> deserializedReadSet = deserializedTransaction.getReadSet();
		ArrayList<Operation> deserializedWriteSet = deserializedTransaction.getWriteSet();
		
		assertEquals(1, deserializedReadSet.size());
		assertEquals(1, deserializedWriteSet.size());
		
		Operation writeOperation = deserializedWriteSet.get(0);
		assertEquals(Operation.Type.WRITE, writeOperation.getType());
		assertEquals("key1", writeOperation.getKey());
		HashMap<String, String> writeColumnValues = writeOperation.getColumnValues();
		assertEquals("value1", writeColumnValues.get("column1"));
		assertEquals("value2", writeColumnValues.get("column2"));
		
		Operation readOperation = deserializedReadSet.get(0);
		assertEquals(Operation.Type.READ, readOperation.getType());
		assertEquals("key2", readOperation.getKey());
		HashMap<String, String> readColumnValues = readOperation.getColumnValues();
		assertEquals("value3", writeColumnValues.get("column3"));
		assertEquals("value4", writeColumnValues.get("column4"));
	}

}
