package edu.ucsb.rc.locks;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

}
