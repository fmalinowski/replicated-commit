package edu.ucsb.rc.locks;

import java.util.HashMap;

public class LocksManager {
	public enum LockType {
		SHARED_LOCK, 
		EXCLUSIVE_LOCK
	}
	
	private class DataObjectLock {
		public String serverTransactionId;
		public LockType lockType;
	}
	
	HashMap<String, DataObjectLock> locks;
	
	public LocksManager() {
		this.locks = new HashMap<String, DataObjectLock>();
	}
	
	// Return true if the shared lock was acquired
	public synchronized boolean addSharedLock(String key, String serverTransactionId) {
		// TODO
		return false;
	}
	
	public synchronized boolean isSharedLock(String key, String serverTransactionId) {
		// TODO
		return false;
	}
	
	// Return true if Exclusive lock was acquired (can convert shared lock to exclusive lock)
	public synchronized boolean addExclusiveLock(String key, String serverTransactionId) {
		// TODO
		return false;
	}
	
	public synchronized boolean isExclusiveLocked(String key, String serverTransactionId) {
		// TODO
		return false;
	}
	
	public synchronized void removeLock(String key, String serverTransactionId) {
		// TODO
	}
}
