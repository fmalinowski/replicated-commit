package edu.ucsb.rc.transactions;

import java.io.Serializable;
import java.util.ArrayList;

public class Transaction implements Serializable {
	@Override
	public String toString() {
		return "Transaction [clientTransactionID=" + clientTransactionID
				+ ", serverTransactionId=" + serverTransactionId
				+ ", clientIpAddress=" + clientIpAddress + ", clientPort="
				+ clientPort + ", readSet=" + readSet + ", writeSet="
				+ writeSet + "]";
	}

	private static final long serialVersionUID = 4547150326687933596L;
	
	private long clientTransactionID;
	private String serverTransactionId;
	private String clientIpAddress;
	private int clientPort;
	
	private ArrayList<Operation> readSet;
	private ArrayList<Operation> writeSet;
	
	public void setTransactionIdDefinedByClient(long transactionID) {
		this.clientTransactionID = transactionID;
	}
	
	public long getTransactionIdDefinedByClient() {
		return this.clientTransactionID;
	}
	
	public void setServerTransactionId(String clientIpAddress, int clientPort, long clientTransactionId) {
		this.clientIpAddress = clientIpAddress;
		this.clientPort = clientPort;
		this.serverTransactionId = buildServerTransactionId(clientIpAddress, clientPort, clientTransactionId);
	}
	
	public String getServerTransactionId() {
		return this.serverTransactionId;
	}
	
	public String getClientIpAddress() {
		return this.clientIpAddress;
	}
	
	public int getClientPort() {
		return this.clientPort;
	}
	
	private String buildServerTransactionId(String clientIpAddress, int clientPort, long clientTransactionId) {
		return clientIpAddress + "/" + clientPort + "/" + clientTransactionId;
	}

	public ArrayList<Operation> getReadSet() {
		return readSet;
	}

	public void setReadSet(ArrayList<Operation> readSet) {
		this.readSet = readSet;
	}

	public ArrayList<Operation> getWriteSet() {
		return writeSet;
	}

	public void setWriteSet(ArrayList<Operation> writeSet) {
		this.writeSet = writeSet;
	}
}
