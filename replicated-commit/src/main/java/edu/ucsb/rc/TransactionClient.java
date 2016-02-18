package edu.ucsb.rc;

public class TransactionClient {
	private String ipAddress;
	private int port;
	private long transactionID;
	
	public TransactionClient(String clientIpAddress, int clientPort, long transactionIdDefinedByClient) {
		this.ipAddress = clientIpAddress;
		this.port = clientPort;
		this.transactionID = transactionIdDefinedByClient;
	}

	public String getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	} 

	public void setTransactionIdDefinedByClient(long transactionID) {
		this.transactionID = transactionID;
	}
	
	public long getTransactionIdDefinedByClient() {
		return this.transactionID;
	}
	
	public String getServerSideTransactionID() {
		return buildServerSideTransactionID(this.ipAddress, this.port, this.transactionID);
	}
	
	public static String buildServerSideTransactionID(String ip, int port, long transactionID) {
		return ip + "/" + port + "/" + transactionID;
	}
}
