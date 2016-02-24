package edu.ucsb.rc.protocols;

import java.util.HashMap;

import edu.ucsb.rc.transactions.Transaction;

public class TwoPhaseCommitManager {
	private HashMap<String, Integer> twoPCacceptedRequestsMap;
	private HashMap<String, Long> twoPCtimestamps;
	private int shardsNumberPerDatacenter;
	
	public TwoPhaseCommitManager(int shardsNumberPerDatacenter) {
		this.twoPCacceptedRequestsMap = new HashMap<String, Integer>();
		this.twoPCtimestamps = new HashMap<String, Long>();
		this.shardsNumberPerDatacenter = shardsNumberPerDatacenter;
	}
	
	public synchronized void startTracking2PCaccepts(Transaction t) {
		this.twoPCacceptedRequestsMap.put(t.getServerTransactionId(), 0);
		this.twoPCtimestamps.put(t.getServerTransactionId(), System.currentTimeMillis());
	}
	
	public synchronized boolean signalAcceptedPrepare(Transaction t) {
		if (!this.twoPCacceptedRequestsMap.containsKey(t.getServerTransactionId())) {
			return false;
		}
		int numberOfAcceptations = this.twoPCacceptedRequestsMap.get(t.getServerTransactionId()) + 1;
		
		this.twoPCacceptedRequestsMap.put(t.getServerTransactionId(), numberOfAcceptations);
		
		return this.is2PCRequestAcceptedByEverybody(t);
	}
	
	public synchronized boolean is2PCRequestAcceptedByEverybody(Transaction t) {
		if (!this.twoPCacceptedRequestsMap.containsKey(t.getServerTransactionId())) {
			return false;
		}
		int numberOfAcceptations = this.twoPCacceptedRequestsMap.get(t.getServerTransactionId());
		
		return (numberOfAcceptations == this.shardsNumberPerDatacenter) ? true : false;
	}
}
