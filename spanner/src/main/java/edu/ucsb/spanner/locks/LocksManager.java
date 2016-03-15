package edu.ucsb.spanner.locks;

import java.util.ArrayList;
import java.util.HashMap;

import edu.ucsb.spanner.model.Operation;
import edu.ucsb.spanner.model.Transaction;

public class LocksManager {
	public enum LockType {
		SHARED_LOCK, 
		EXCLUSIVE_LOCK
	}
	
	private class DataObjectLock {
		public String serverTransactionId;
		public LockType lockType;
		
		public DataObjectLock(String serverTransactionId, LockType lockType) {
			this.serverTransactionId = serverTransactionId;
			this.lockType = lockType;
		}
	}
	
	HashMap<String, DataObjectLock> locks;
	
	public LocksManager() {
		this.locks = new HashMap<String, DataObjectLock>();
	}
	
	/*
	 *  Return true if the shared lock was acquired
	 *  Return false also if a shared lock was acquired by another transaction (Replicated commit rule)
	 */
	public synchronized boolean addSharedLock(String key, String serverTransactionId) {
		DataObjectLock lock;
		if (this.locks.containsKey(key)) {
			lock = this.locks.get(key);
			if (lock.lockType == LockType.SHARED_LOCK && lock.serverTransactionId.equals(serverTransactionId)) {
				return true;
			}
			return false;
		}
		lock = new DataObjectLock(serverTransactionId, LockType.SHARED_LOCK);
		this.locks.put(key, lock);
		return true;
	}
	
	/*
	 * Return true if Exclusive lock was acquired 
	 * (can convert shared lock even if it belongs to another transaction to exclusive lock)
	 * but it cannot acquire an exclusive lock that is already locked by another txn. 
	 */
	public synchronized boolean addExclusiveLock(String key, String serverTransactionId) {
		DataObjectLock lock;
		if (this.locks.containsKey(key)) {
			lock = this.locks.get(key);
			if (lock.serverTransactionId.equals(serverTransactionId)) {
				lock.lockType = LockType.EXCLUSIVE_LOCK;
				return true;
			}
			if (lock.lockType == LockType.SHARED_LOCK) {
				lock.lockType = LockType.EXCLUSIVE_LOCK;
				lock.serverTransactionId = serverTransactionId;
				return true;
			}
			return false;
		}
		lock = new DataObjectLock(serverTransactionId, LockType.EXCLUSIVE_LOCK);
		this.locks.put(key, lock);
		return true;
	}
	
	/*
	 * Return true if it's shared locked or exclusively locked
	 */
	public synchronized boolean isLocked(String key) {
		return this.locks.containsKey(key);
	}

	/*
	 * Return true if it's shared locked or exclusively locked by a given transaction
	 */
	public synchronized boolean isLockedByTransaction(String key, String serverTransactionId) {
		DataObjectLock lock;
		if (this.locks.containsKey(key)) {
			lock = this.locks.get(key);
			return lock.serverTransactionId.equals(serverTransactionId) ? true : false; 
		}
		return false;
	}
	
	/*
	 * Return true if the lock was removed successfully (lock belonged to the right transaction)
	 */
	public synchronized boolean removeLock(String key, String serverTransactionId) {
		DataObjectLock lock;
		if (this.locks.containsKey(key)) {
			lock = this.locks.get(key);
			if (lock.serverTransactionId.equals(serverTransactionId)) {
				this.locks.remove(key);
				return true;
			}
			return false;
		}
		return true;
	}
	
	/*
	 * Return true if all shared locks (read locks) are still acquired for a transaction
	 */
	public synchronized boolean checkSharedLocksAreStillAcquiredForTxn(Transaction t) {
		ArrayList<Operation> readSet = t.getReadSet();
		
		if (readSet == null) {
			return true;
		}
		
		for (Operation readOp : readSet) {
			if (!this.isLockedByTransaction(readOp.getKey(), t.getServerTransactionId())) {
				return false;
			}
		}
		return true;
	}
	
	/*
	 * Return true if all exclusive locks (write locks) are acquired for a transaction
	 * This method doesn't have to be thread safe
	 */
	public boolean acquireExclusiveLocksForTxn(Transaction t) {
		ArrayList<Operation> writeSet = t.getWriteSet();
		
		if (writeSet == null) {
			return true;
		}
		
		for (Operation writeOp : writeSet) {
			if (!this.addExclusiveLock(writeOp.getKey(), t.getServerTransactionId())) {
				return false;
			}
		}
		return true;
	}
	
	/*
	 * Remove all the locks (shared locks and exclusive locks) for a transaction
	 * This method doesn't have to be thread safe
	 */
	public void removeAllLocksForTxn(Transaction t) {
		this.removeAllSharedLocksForTxn(t);
		ArrayList<Operation> writeSet = t.getWriteSet();
		
		if (writeSet == null) {
			return;
		}
		
		for (Operation writeOp : writeSet) {
			this.removeLock(writeOp.getKey(), t.getServerTransactionId());
		}
	}
	
	/*
	 * Remove all the shared locks for a transaction
	 * This method doesn't have to be thread safe
	 */
	public void removeAllSharedLocksForTxn(Transaction t) {
		ArrayList<Operation> readSet = t.getReadSet();
		
		if (readSet == null) {
			return;
		}
		
		for (Operation readOp : readSet) {
			this.removeLock(readOp.getKey(), t.getServerTransactionId());
		}
	}
}
