package edu.ucsb.spanner.protocols;

import java.util.HashMap;

import edu.ucsb.rc.model.Transaction;

public class PaxosManager {
	
	
	public void logTwoPhaseCommitLocally()
	{
		
	}
	
	public void replicateLogLocally()
	{
		
	}
	
	private int datacentersNumber;
	private HashMap<String, Integer> paxosAcceptAcceptedMap;
	private HashMap<String, Long> paxosAcceptTimestamps;
	
	public PaxosManager(int datacentersNumber) {
		this.datacentersNumber = datacentersNumber;
		this.paxosAcceptAcceptedMap = new HashMap<String, Integer>();
		this.paxosAcceptTimestamps = new HashMap<String, Long>();
	}
	
	public synchronized boolean increaseAcceptAccepted(Transaction t) {
		String txnId = t.getServerTransactionId();
		int numberOfAccepts;
		
		if (!this.paxosAcceptAcceptedMap.containsKey(t.getServerTransactionId())) {
			numberOfAccepts = 1;
			this.paxosAcceptTimestamps.put(txnId, System.currentTimeMillis());
		} else {
			numberOfAccepts = this.paxosAcceptAcceptedMap.get(txnId) + 1;
		}
		this.paxosAcceptAcceptedMap.put(txnId, numberOfAccepts);
		return this.isPaxosAcceptAcceptedByMajority(t);
	}
	
	public synchronized int getAcceptAcceptedNumber(Transaction t) {
		int numberOfAccepts;
		
		if (this.paxosAcceptAcceptedMap.containsKey(t.getServerTransactionId())) {
			numberOfAccepts = this.paxosAcceptAcceptedMap.get(t.getServerTransactionId()); 
		} else {
			numberOfAccepts = 0;
		}
		return numberOfAccepts;
	}
	
	public synchronized boolean isPaxosAcceptAcceptedByMajority(Transaction t) {
		int numberOfAccepts = this.getAcceptAcceptedNumber(t);
		int thresholdForMajority = (int) Math.ceil((double)this.datacentersNumber/2);
		
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