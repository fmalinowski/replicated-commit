package edu.ucsb.rc;

public class TransactionClient {
	private String ipAddress;
	private int port;
	private int transactionID;

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

	public void setTransactionIdDefinedByClient(int transactionID) {
		this.transactionID = transactionID;
	}
	
	public int getTransactionIdDefinedByClient() {
		return this.transactionID;
	}
	
	public String getServerSideTransactionID() {
		return buildServerSideTransactionID(this.ipAddress, this.port, this.transactionID);
	}
	
	public static String buildServerSideTransactionID(String ip, int port, int transactionID) {
		return ip + "/" + port + "/" + transactionID;
	}
}
