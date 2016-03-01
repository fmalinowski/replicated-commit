package edu.ucsb.rc.protocols;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.ucsb.rc.transactions.Transaction;

public class PaxosAcceptsManagerTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testIncreaseAcceptAccepted() {
		PaxosAcceptsManager pam = new PaxosAcceptsManager(3);
		
		Transaction txn1 = new Transaction();
		Transaction txn2 = new Transaction();
		Transaction txn3 = new Transaction();
		
		txn1.setServerTransactionId("1", 1, 1);
		txn2.setServerTransactionId("1", 1, 2);
		txn3.setServerTransactionId("1", 1, 3);
		
		assertFalse(pam.increaseAcceptAccepted(txn1));
		assertEquals(1, pam.getAcceptAcceptedNumber(txn1));
		assertFalse(pam.isPaxosAcceptAcceptedByMajority(txn1));
		
		assertFalse(pam.increaseAcceptAccepted(txn2));
		assertEquals(1, pam.getAcceptAcceptedNumber(txn2));
		assertFalse(pam.isPaxosAcceptAcceptedByMajority(txn2));
		
		assertTrue(pam.increaseAcceptAccepted(txn1));
		assertEquals(2, pam.getAcceptAcceptedNumber(txn1));
		assertTrue(pam.isPaxosAcceptAcceptedByMajority(txn1));
		
		assertTrue(pam.increaseAcceptAccepted(txn2));
		assertEquals(2, pam.getAcceptAcceptedNumber(txn2));
		assertTrue(pam.isPaxosAcceptAcceptedByMajority(txn2));
		
		assertFalse(pam.increaseAcceptAccepted(txn3));
		assertEquals(1, pam.getAcceptAcceptedNumber(txn3));
		assertFalse(pam.isPaxosAcceptAcceptedByMajority(txn3));
		
		assertTrue(pam.increaseAcceptAccepted(txn3));
		assertEquals(2, pam.getAcceptAcceptedNumber(txn3));
		assertTrue(pam.isPaxosAcceptAcceptedByMajority(txn3));
		
		PaxosAcceptsManager pam2 = new PaxosAcceptsManager(5);
		
		assertFalse(pam2.increaseAcceptAccepted(txn1));
		assertEquals(1, pam2.getAcceptAcceptedNumber(txn1));
		assertFalse(pam2.isPaxosAcceptAcceptedByMajority(txn1));
		
		assertFalse(pam2.increaseAcceptAccepted(txn1));
		assertEquals(2, pam2.getAcceptAcceptedNumber(txn1));
		assertFalse(pam2.isPaxosAcceptAcceptedByMajority(txn1));
		
		assertTrue(pam2.increaseAcceptAccepted(txn1));
		assertEquals(3, pam2.getAcceptAcceptedNumber(txn1));
		assertTrue(pam2.isPaxosAcceptAcceptedByMajority(txn1));
		
		assertTrue(pam2.increaseAcceptAccepted(txn1));
		assertEquals(4, pam2.getAcceptAcceptedNumber(txn1));
		assertTrue(pam2.isPaxosAcceptAcceptedByMajority(txn1));
		
		assertTrue(pam2.increaseAcceptAccepted(txn1));
		assertEquals(5, pam2.getAcceptAcceptedNumber(txn1));
		assertTrue(pam2.isPaxosAcceptAcceptedByMajority(txn1));
	}

	@Test
	public void testRemoveTrackOfPaxosAccepts() {
		PaxosAcceptsManager pam = new PaxosAcceptsManager(3);
		
		Transaction txn1 = new Transaction();
		pam.increaseAcceptAccepted(txn1);
		assertEquals(1, pam.getAcceptAcceptedNumber(txn1));
		pam.increaseAcceptAccepted(txn1);
		assertEquals(2, pam.getAcceptAcceptedNumber(txn1));
		
		pam.removeTrackOfPaxosAccepts(txn1);
		assertEquals(0, pam.getAcceptAcceptedNumber(txn1));
	}

}
