package edu.ucsb.rc.locks;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.ucsb.rc.model.Operation;
import edu.ucsb.rc.model.Transaction;

public class LocksManagerTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testAddSharedLock() {
		LocksManager lm = new LocksManager();
		assertTrue(lm.addSharedLock("key1", "txn1"));
		assertTrue(lm.addSharedLock("key2", "txn1"));
		assertTrue(lm.addSharedLock("key2", "txn1"));
		assertTrue(lm.addSharedLock("key3", "txn2"));
		assertFalse(lm.addSharedLock("key1", "txn2"));
		assertFalse(lm.addSharedLock("key3", "txn1"));
	}

	@Test
	public void testAddExclusiveLock() {
		LocksManager lm = new LocksManager();
		assertTrue(lm.addExclusiveLock("key1", "txn1"));
		assertTrue(lm.addExclusiveLock("key2", "txn2"));
		assertTrue(lm.addExclusiveLock("key3", "txn2"));
		assertTrue(lm.addExclusiveLock("key3", "txn2"));
		assertFalse(lm.addExclusiveLock("key3", "txn1"));
		assertFalse(lm.addExclusiveLock("key1", "txn2"));
		
		lm.addSharedLock("key4", "txn1");
		assertTrue(lm.addExclusiveLock("key4", "txn2"));
		
		assertFalse(lm.addSharedLock("key4", "txn1"));
		assertFalse(lm.addSharedLock("key4", "txn2"));
		assertTrue(lm.addExclusiveLock("key4", "txn2"));
		
		lm.addSharedLock("key5", "txn2");
		assertTrue(lm.addExclusiveLock("key5", "txn2"));
	}

	@Test
	public void testIsLocked() {
		LocksManager lm = new LocksManager();
		
		assertFalse(lm.isLocked("key1"));
		lm.addSharedLock("key1", "txn1");
		assertTrue(lm.isLocked("key1"));
		lm.addExclusiveLock("key1", "txn1");
		assertTrue(lm.isLocked("key1"));
		
		assertFalse(lm.isLocked("key2"));
		lm.addExclusiveLock("key2", "txn2");
		assertTrue(lm.isLocked("key2"));
		
		lm.addSharedLock("key3", "txn1");
		assertTrue(lm.isLocked("key3"));
		lm.addSharedLock("key3", "txn2");
		assertTrue(lm.isLocked("key3"));
	}
	
	@Test
	public void testIsLockedByTransaction() {
		LocksManager lm = new LocksManager();
		
		assertFalse(lm.isLockedByTransaction("key1", "txn1"));
		lm.addSharedLock("key1", "txn1");
		assertTrue(lm.isLockedByTransaction("key1", "txn1"));
		assertFalse(lm.isLockedByTransaction("key1", "txn2"));
		
		assertFalse(lm.isLockedByTransaction("key2", "txn2"));
		lm.addExclusiveLock("key2", "txn2");
		assertTrue(lm.isLockedByTransaction("key2", "txn2"));
		assertFalse(lm.isLockedByTransaction("key2", "txn1"));
		
		lm.addSharedLock("key3", "txn1");
		assertTrue(lm.isLockedByTransaction("key3", "txn1"));
		assertFalse(lm.isLockedByTransaction("key3", "txn2"));
		lm.addSharedLock("key3", "txn2");
		assertTrue(lm.isLockedByTransaction("key3", "txn1"));
		assertFalse(lm.isLockedByTransaction("key3", "txn2"));
		lm.addExclusiveLock("key3", "txn2");
		assertFalse(lm.isLockedByTransaction("key3", "txn1"));
		assertTrue(lm.isLockedByTransaction("key3", "txn2"));
	}

	@Test
	public void testRemoveLock() {
		LocksManager lm = new LocksManager();
		
		assertTrue(lm.removeLock("key1", "txn1"));
		lm.addSharedLock("key1", "txn1");
		assertTrue(lm.removeLock("key1", "txn1"));
		assertFalse(lm.isLocked("key1"));
		
		assertFalse(lm.isLocked("key2"));
		lm.addExclusiveLock("key2", "txn1");
		assertTrue(lm.removeLock("key2", "txn1"));
		assertFalse(lm.isLocked("key2"));
		
		lm.addSharedLock("key1", "txn1");
		assertTrue(lm.isLocked("key1"));
		lm.addExclusiveLock("key1", "txn2");
		assertFalse(lm.removeLock("key1", "txn1"));
		assertTrue(lm.removeLock("key1", "txn2"));
	}

	@Test
	public void testCheckSharedLocksAreStillAcquiredForTxn() {
		LocksManager lm = new LocksManager();
		
		Transaction t1 = new Transaction();
		t1.setServerTransactionId("ip1", 12345, 1);
		ArrayList<Operation> t1readlist = new ArrayList<Operation>();
		t1.setReadSet(t1readlist);
		
		Transaction t1read1 = new Transaction();
		t1read1.setServerTransactionId("ip1", 12345, 1);
		ArrayList<Operation> t1read1list = new ArrayList<Operation>();
		t1read1.setReadSet(t1read1list);
		
		Operation readOp1 = new Operation();
		readOp1.setKey("a");
		readOp1.setType(Operation.Type.READ);
		t1read1list.add(readOp1);
		t1readlist.add(readOp1);
		
		Transaction t1read2 = new Transaction();
		t1read2.setServerTransactionId("ip1", 12345, 1);
		ArrayList<Operation> t1read2list = new ArrayList<Operation>();
		t1read2.setReadSet(t1read2list);
		
		Operation readOp2 = new Operation();
		readOp2.setKey("b");
		readOp2.setType(Operation.Type.READ);
		t1read2list.add(readOp2);
		t1readlist.add(readOp2);
		
		Transaction t2 = new Transaction();
		t2.setServerTransactionId("ip1", 12345, 2);
		ArrayList<Operation> t2writelist = new ArrayList<Operation>();
		t2.setWriteSet(t2writelist);
		
		Operation writeOp1 = new Operation();
		writeOp1.setKey("b");
		writeOp1.setType(Operation.Type.WRITE);
		t2writelist.add(writeOp1);
		
		lm.addSharedLock("a", t1read1.getServerTransactionId());
		lm.addSharedLock("b", t1read1.getServerTransactionId());
		
		assertTrue(lm.checkSharedLocksAreStillAcquiredForTxn(t1));
		
		lm.removeAllSharedLocksForTxn(t1);
		assertFalse(lm.checkSharedLocksAreStillAcquiredForTxn(t1));
		
		lm.addSharedLock("a", t1read1.getServerTransactionId());
		lm.addSharedLock("b", t1read1.getServerTransactionId());
		
		assertTrue(lm.checkSharedLocksAreStillAcquiredForTxn(t1));
		
		lm.acquireExclusiveLocksForTxn(t2);
		assertFalse(lm.checkSharedLocksAreStillAcquiredForTxn(t1));
	}
}
