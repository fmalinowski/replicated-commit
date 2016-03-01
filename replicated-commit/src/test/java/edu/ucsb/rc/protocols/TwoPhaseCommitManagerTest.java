package edu.ucsb.rc.protocols;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.ucsb.rc.transactions.Transaction;

public class TwoPhaseCommitManagerTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}
	

	@Test
	public void testSignalAcceptedPrepare() {
		TwoPhaseCommitManager tpcm = new TwoPhaseCommitManager(3);
		
		Transaction txn1 = new Transaction();
		Transaction txn2 = new Transaction();
		txn1.setServerTransactionId("1", 1, 1);
		txn2.setServerTransactionId("1", 1, 2);
		
		assertFalse(tpcm.signalAcceptedPrepare(txn1));
		assertFalse(tpcm.signalAcceptedPrepare(txn1));
		assertFalse(tpcm.signalAcceptedPrepare(txn1));
		
		tpcm.startTracking2PCaccepts(txn1);
		assertFalse(tpcm.signalAcceptedPrepare(txn1));
		assertFalse(tpcm.is2PCRequestAcceptedByEverybody(txn1));
		
		tpcm.startTracking2PCaccepts(txn2);
		assertFalse(tpcm.signalAcceptedPrepare(txn1));
		assertFalse(tpcm.is2PCRequestAcceptedByEverybody(txn1));
		assertFalse(tpcm.signalAcceptedPrepare(txn2));
		assertFalse(tpcm.is2PCRequestAcceptedByEverybody(txn2));
		
		assertTrue(tpcm.signalAcceptedPrepare(txn1));
		assertTrue(tpcm.is2PCRequestAcceptedByEverybody(txn1));
		assertFalse(tpcm.signalAcceptedPrepare(txn2));
		assertFalse(tpcm.is2PCRequestAcceptedByEverybody(txn2));
		
		assertTrue(tpcm.signalAcceptedPrepare(txn1));
		assertTrue(tpcm.is2PCRequestAcceptedByEverybody(txn1));
		assertTrue(tpcm.signalAcceptedPrepare(txn2));
		assertTrue(tpcm.is2PCRequestAcceptedByEverybody(txn2));
		
		TwoPhaseCommitManager tpcm2 = new TwoPhaseCommitManager(4);
		
		Transaction txn3 = new Transaction();
		
		tpcm2.startTracking2PCaccepts(txn3);
		assertFalse(tpcm2.signalAcceptedPrepare(txn3));
		assertFalse(tpcm2.is2PCRequestAcceptedByEverybody(txn3));
		assertFalse(tpcm2.signalAcceptedPrepare(txn3));
		assertFalse(tpcm2.is2PCRequestAcceptedByEverybody(txn3));
		assertFalse(tpcm2.signalAcceptedPrepare(txn3));
		assertFalse(tpcm2.is2PCRequestAcceptedByEverybody(txn3));
		assertTrue(tpcm2.signalAcceptedPrepare(txn3));
		assertTrue(tpcm2.is2PCRequestAcceptedByEverybody(txn3));
	}
	
	@Test
	public void testStopTracking2PCaccepts() {
		TwoPhaseCommitManager tpcm = new TwoPhaseCommitManager(3);
		
		Transaction txn1 = new Transaction();
		tpcm.startTracking2PCaccepts(txn1);
		tpcm.signalAcceptedPrepare(txn1);
		tpcm.is2PCRequestAcceptedByEverybody(txn1);
		tpcm.signalAcceptedPrepare(txn1);
		tpcm.is2PCRequestAcceptedByEverybody(txn1);
		assertTrue(tpcm.signalAcceptedPrepare(txn1));
		assertTrue(tpcm.is2PCRequestAcceptedByEverybody(txn1));
		
		tpcm.stopTracking2PCaccepts(txn1);
		assertFalse(tpcm.signalAcceptedPrepare(txn1));
		assertFalse(tpcm.is2PCRequestAcceptedByEverybody(txn1));
		assertFalse(tpcm.signalAcceptedPrepare(txn1));
		assertFalse(tpcm.is2PCRequestAcceptedByEverybody(txn1));
		assertFalse(tpcm.signalAcceptedPrepare(txn1));
		assertFalse(tpcm.is2PCRequestAcceptedByEverybody(txn1));
		assertFalse(tpcm.signalAcceptedPrepare(txn1));
		assertFalse(tpcm.is2PCRequestAcceptedByEverybody(txn1));
	}
}
