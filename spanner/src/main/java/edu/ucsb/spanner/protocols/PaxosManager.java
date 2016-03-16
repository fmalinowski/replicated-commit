package edu.ucsb.spanner.protocols;

import java.util.HashMap;

import edu.ucsb.spanner.model.Transaction;

public class PaxosManager {

	private int datacentersNumber;
	private HashMap<String, Integer> paxosAcceptAcceptedMap;
	private HashMap<String, Long> paxosAcceptTimestamps;
	private HashMap<String, Integer> paxosTicksForTransaction;

	public PaxosManager(int datacentersNumber) {
		this.datacentersNumber = datacentersNumber;
		this.paxosAcceptAcceptedMap = new HashMap<String, Integer>();
		this.paxosAcceptTimestamps = new HashMap<String, Long>();
		this.paxosTicksForTransaction = new HashMap<String, Integer>();
	}

	public void startNewPaxosSession(Transaction previousTransaction) {
		/*if (previousTransaction != null) {
			// Remove only if the server id exists
			// TODO
			removeTrackOfPaxosAccepts(previousTransaction);
		}*/
	}

	public synchronized boolean increaseAcceptAccepted(Transaction t) {
		/*String txnId = t.getServerTransactionId();
		int numberOfAccepts;
		int ticks;

		if (!this.paxosAcceptAcceptedMap
				.containsKey(t.getServerTransactionId())) {
			numberOfAccepts = 1;
			ticks = 1;
			this.paxosAcceptTimestamps.put(txnId, System.currentTimeMillis());
		} else {
			numberOfAccepts = this.paxosAcceptAcceptedMap.get(txnId) + 1;
			ticks = this.paxosTicksForTransaction.get(txnId) + 1;
		}
		this.paxosAcceptAcceptedMap.put(txnId, numberOfAccepts);
		this.paxosTicksForTransaction.put(txnId, ticks);
		return this.isPaxosAcceptAcceptedByMajority(t);*/
		
		return true;
	}

	public synchronized boolean increaseTicks(Transaction t) {
		/*String txnId = t.getServerTransactionId();
		int ticks;

		if (!this.paxosAcceptAcceptedMap
				.containsKey(t.getServerTransactionId())) {
			ticks = 1;
			this.paxosAcceptTimestamps.put(txnId, System.currentTimeMillis());
		} else {
			ticks = this.paxosTicksForTransaction.get(txnId) + 1;
		}
		this.paxosTicksForTransaction.put(txnId, ticks);
		return (this.paxosTicksForTransaction.get(txnId) == datacentersNumber);*/
		
		return false;
	}

	private synchronized int getAcceptAcceptedNumber(Transaction t) {
		int numberOfAccepts;

		if (this.paxosAcceptAcceptedMap.containsKey(t.getServerTransactionId())) {
			numberOfAccepts = this.paxosAcceptAcceptedMap.get(t
					.getServerTransactionId());
		} else {
			numberOfAccepts = 0;
		}
		return numberOfAccepts;
	}

	public synchronized boolean isPaxosAcceptAcceptedByMajority(Transaction t) {
		int numberOfAccepts = this.getAcceptAcceptedNumber(t);
		int thresholdForMajority = (int) Math
				.ceil((double) this.datacentersNumber / 2);

		if (numberOfAccepts >= thresholdForMajority) {
			return true;
		}
		return false;
	}

	public synchronized void removeTrackOfPaxosAccepts(Transaction t) {
		this.paxosAcceptAcceptedMap.remove(t.getServerTransactionId());
		this.paxosAcceptTimestamps.remove(t.getServerTransactionId());
	}
}
