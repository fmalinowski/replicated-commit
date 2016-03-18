package com.yahoo.ycsb.db;

import static com.yahoo.ycsb.Status.ERROR;
import static com.yahoo.ycsb.Status.NOT_IMPLEMENTED;
import static com.yahoo.ycsb.Status.OK;

import java.io.FileInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Logger;

import com.yahoo.ycsb.*;

import edu.ucsb.rc.model.*;

public class ReplicatedCommit extends DB {

	private Transaction currentTransaction;
	private Long transactionId;

	private final Logger LOGGER;
	
	private int datacentersNumber;
	private int shardsPerDatacenter;
	private int acceptanceCriteria;
	private int shardPort;
	private HashMap<Integer, String> ipMap;
	private DatagramSocket socket;
	
	private int abortedTransactions = 0;
	private int commitedTransactions = 0;

	private static final int BUFFER_SIZE = 65507;
	private static final String CLASS_NAME = ReplicatedCommit.class.getName();

	private ArrayList<Operation> currentReadSet;
	private ArrayList<Operation> currentWriteSet;
	private Random randomGenerator;

	public ReplicatedCommit() {
		LOGGER = Logger.getLogger(CLASS_NAME);
		randomGenerator = new Random();
		this.ipMap = new HashMap<Integer, String>();
		
		readConfigFileAndInitialize();
	}

	@Override
	public void init() throws DBException {
		transactionId = (long)0;

		try {
			this.socket = new DatagramSocket();
			this.socket.setSoTimeout(1000); // Timeout for 1000ms

		} catch (SocketException e) {
			e.printStackTrace();
		}

		LOGGER.info("------Init method---Thread"
				+ Thread.currentThread().getId() + " ---Transaction Id "
				+ transactionId);

	}

	@Override
	public void cleanup() throws DBException {
		LOGGER.info("------CleanUp method---Thread"
				+ Thread.currentThread().getId() + " ---Transaction Id "
				+ transactionId);
	}

	@Override
	public void start() throws DBException {
		transactionId++;
		LOGGER.info("------Start method---Thread"
				+ Thread.currentThread().getId() + " ---Transaction Id "
				+ transactionId);
		currentTransaction = new Transaction();
		currentTransaction.setTransactionIdDefinedByClient(transactionId);
		
		currentReadSet = new ArrayList<Operation>();
		currentWriteSet = new ArrayList<Operation>();
		currentTransaction.setReadSet(currentReadSet);
		currentTransaction.setWriteSet(currentWriteSet);
	}
	
	@Override
	public Status read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {

		LOGGER.info("------Read method---Thread"
				+ Thread.currentThread().getId() + " ---Transaction Id "
				+ transactionId);

		Operation readOperationAnswerFromServer;
		
		Transaction readTransaction = new Transaction();
		readTransaction.setTransactionIdDefinedByClient(currentTransaction.getTransactionIdDefinedByClient());
		
		ArrayList<Operation> readSet = new ArrayList<Operation>();
		readTransaction.setReadSet(readSet);
		
		Operation readOperation = new Operation();
		readOperation = fillInReadOperation(key, fields);
		readSet.add(readOperation);
		
		// Add read operation to current ReadSet of the current transaction that will be sent
		// at commit time
		currentReadSet.add(readOperation);
		
		readOperationAnswerFromServer = sendReadRequestToShards(readTransaction);
		
		if (readOperationAnswerFromServer == null) {
			// Either the number of positive answers was not above acceptance criteria
			// or we got a message that was not an answer for a read request
			// We need to abort the transaction here!!
			this.abortedTransactions++;
			return Status.ERROR;
		}
		
		convertReadValuesToYCSBformat(readOperationAnswerFromServer.getColumnValues(), result);

		return Status.OK;
	}
	
	@Override
	public Status update(String table, String key,
			HashMap<String, ByteIterator> values) {

		LOGGER.info("------Update method---Thread"
				+ Thread.currentThread().getId() + " ---Transaction Id "
				+ transactionId);

		Operation writeOperation = new Operation();
		HashMap<String, String> columnValues = new HashMap<String, String>();
		writeOperation.setKey(key);
		writeOperation.setType(Operation.Type.WRITE);
		writeOperation.setColumnValues(columnValues);
		
		convertYCSBwriteValuesToInternalFormat(values, columnValues);
		currentWriteSet.add(writeOperation);

		return Status.OK;
	}

	@Override
	public Status insert(String table, String key,
			HashMap<String, ByteIterator> values) {

		LOGGER.info("------Insert method---Thread"
				+ Thread.currentThread().getId() + " ---Transaction Id "
				+ transactionId);

		Operation writeOperation = new Operation();
		HashMap<String, String> columnValues = new HashMap<String, String>();
		writeOperation.setKey(key);
		writeOperation.setType(Operation.Type.WRITE);
		writeOperation.setColumnValues(columnValues);
		
		convertYCSBwriteValuesToInternalFormat(values, columnValues);
		currentWriteSet.add(writeOperation);

		return Status.OK;

	}

	@Override
	public void commit() throws DBException {
		int coordinatorShardId, acceptedPaxosRequests;
		String coordinatorShardIp;
		Message message, answerFromShard;
		
		if (currentTransaction == null) {
			return;
		}
		
		currentTransaction.setWriteSet(currentWriteSet);
		
		message = new Message();
		message.setMessageType(Message.MessageType.PAXOS__ACCEPT_REQUEST);
		message.setTransaction(currentTransaction);
		
		coordinatorShardId = this.getCoordinatorShardID();
		
		acceptedPaxosRequests = 0;
		
		//for (int datacenterID = 0; datacenterID < this.datacentersNumber; datacenterID++) {
			//coordinatorShardIp = this.getIpForShard(datacenterID, coordinatorShardId);
			
			this.sendMessageToShard(message, "128.111.43.14");
			LOGGER.info("------Commit sent message to shard "
					+ "128.111.43.14" + " ---Transaction Id "
					+ transactionId);
		//}
		
		for (int datacenterID = 0; datacenterID < this.datacentersNumber; datacenterID++) {
			coordinatorShardIp = this.getIpForShard(datacenterID, coordinatorShardId);
			
			do {
				answerFromShard = receiveMessageFromShards();
				if (answerFromShard == null) {
					this.abortedTransactions++;
					return;
				}
			} while (answerFromShard.getTransaction().getTransactionIdDefinedByClient() != this.transactionId);
			
			if (answerFromShard.getTransaction().getTransactionIdDefinedByClient() == this.transactionId && answerFromShard.getMessageType() == Message.MessageType.PAXOS__ACCEPT_REQUEST_ACCEPTED) {
				acceptedPaxosRequests++;
			}
			else if (answerFromShard.getTransaction().getTransactionIdDefinedByClient() == this.transactionId && answerFromShard.getMessageType() == Message.MessageType.PAXOS__ACCEPT_REQUEST_DENIED) {
					
			} else {
				// There's a big problem here cause we received a message not related to our request
				// Might be due response to another request...
				LOGGER.info("------Commit method---Thread"
						+ Thread.currentThread().getId() + " ---Transaction Id "
						+ transactionId
						+ " --- GOT WRONG MESSAGE - PROBLEM! -- We got this type of message:" + answerFromShard.getMessageType() + " | txnID received: " + answerFromShard.getTransaction().getTransactionIdDefinedByClient());
				return;
			}
		}
		
		String logString;
		
		if (acceptedPaxosRequests >= this.acceptanceCriteria) {
			// Transaction has successfully committed
			this.commitedTransactions++;
			logString = "Commited Successfuly";
		} else {
			this.abortedTransactions++;
			logString = "Txn Aborted";
		}
		
		LOGGER.info("------Commit method---Thread"
				+ Thread.currentThread().getId() + " ---Transaction Id "
				+ transactionId + "Current Write Set " + currentWriteSet.size()
				+ " - " + logString);

		double commitsPercentage = (double)this.commitedTransactions / (this.abortedTransactions + this.commitedTransactions) * 100;
		LOGGER.info("++++ Summary ++++" 
				+ "Commits: " + this.commitedTransactions
				+ "Aborts: " + this.abortedTransactions
				+ "commits %: " + commitsPercentage);
	}

	@Override
	public void abort() throws DBException {

		LOGGER.info("------Abort method---Thread"
				+ Thread.currentThread().getId() + " ---Transaction Id "
				+ transactionId);

	}

	@Override
	public Status scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

		LOGGER.info("------Scan method---Thread"
				+ Thread.currentThread().getId() + " ---Transaction Id "
				+ transactionId);
		return NOT_IMPLEMENTED;
	}

	@Override
	public Status delete(String table, String key) {

		LOGGER.info("------Delete method---Thread"
				+ Thread.currentThread().getId() + " ---Transaction Id "
				+ transactionId);
		return NOT_IMPLEMENTED;
	}
	
	public void readConfigFileAndInitialize() {
		String configFilePath = "config.properties";
		String shardIPAddress;
		
		FileInputStream file;
		Properties properties = null;
		
		try {
			file = new FileInputStream(configFilePath);
			properties = new Properties();
			properties.load(file);
			file.close();
		} catch (Exception e) {
			e.printStackTrace();
		}	
	
		this.shardPort = Integer.parseInt(properties.getProperty("shardPort"));
		this.setDatacentersNumber(Integer.parseInt(properties.getProperty("numberDatacenters")));
		this.setShardsPerDatacenter(Integer.parseInt(properties.getProperty("numberShardsPerDatacenter")));
		this.acceptanceCriteria = (int) Math.ceil((double)this.datacentersNumber/2);
		
		for (int datacenterID = 0; datacenterID < datacentersNumber; datacenterID++) {
			for (int shardID = 0; shardID < shardsPerDatacenter; shardID++) {
				shardIPAddress = properties.getProperty("DC" + datacenterID + "-Shard" + shardID);
				this.setIpForShard(datacenterID, shardID, shardIPAddress);
			}
		}
	}
	
	public int getDatacentersNumber() {
		return this.datacentersNumber;
	}
	
	public void setDatacentersNumber(int datacentersNumber) {
		this.datacentersNumber = datacentersNumber;
	}
	
	public int getShardsPerDatacenter() {
		return this.shardsPerDatacenter;
	}
	
	public void setShardsPerDatacenter(int shardsPerDatacenter) {
		this.shardsPerDatacenter = shardsPerDatacenter;
	}
	
	public void setIpForShard(int datacenterID, int shardID, String shardIpAddress) {
		int indexOfShardInIpMap;
		
		indexOfShardInIpMap = datacenterID * this.getShardsPerDatacenter() + shardID;
		this.ipMap.put(indexOfShardInIpMap, shardIpAddress);
	}
	
	public String getIpForShard(int datacenterID, int shardID) {
		/*int indexOfShardInIpMap;
		
		indexOfShardInIpMap = datacenterID * this.getShardsPerDatacenter() + shardID;
		if (this.ipMap.containsKey(indexOfShardInIpMap)) {
			return this.ipMap.get(indexOfShardInIpMap);
		}*/
		return "128.111.43.14";
	}
	
	public int getShardIdHoldingData(String key) {
		if (key == null) {
			return -1;
		}
		return (key.hashCode() % this.shardsPerDatacenter);
	}
	
	public Operation fillInReadOperation(String key, Set<String> fields) {
		Operation readOperation = new Operation();
		HashMap<String, String> fieldsMap = new HashMap<String, String>();
		
		readOperation.setType(Operation.Type.READ);
		readOperation.setKey(key);
		readOperation.setColumnValues(fieldsMap);
		
		if (fields != null) {
			for (String field : fields) {
				fieldsMap.put(field, "");
			}
		}
		return readOperation;
	}
	
	public Operation sendReadRequestToShards(Transaction readTransaction) {
		String operationKey, shardIpAddress;
		int shardIdHoldingData, positiveReadAnswers;
		long mostRecentTimestamp;
		Operation readOperationFromServer, bestReadOperationFromShards;
		Message message, answerFromShard;
		
		operationKey = readTransaction.getReadSet().get(0).getKey();
		shardIdHoldingData = this.getShardIdHoldingData(operationKey);
		
		message = new Message();
		message.setMessageType(Message.MessageType.READ_REQUEST);
		message.setTransaction(readTransaction);
		
		positiveReadAnswers = 0;
		mostRecentTimestamp = 0;
		bestReadOperationFromShards = null;
		
		//for (int datacenterID = 0; datacenterID < this.datacentersNumber; datacenterID++) {
			shardIpAddress = "128.111.43.14";//this.getIpForShard(datacenterID, shardIdHoldingData);
			
			this.sendMessageToShard(message, shardIpAddress);
			
			// We might get some messages coming from previous transactions because
			// a txn was PAXOS accepted by all the servers before a coordinator actually
			// got the PAXOS accept request from the client.
			
			
			do {
				answerFromShard = receiveMessageFromShards();
				if (answerFromShard == null) {
					return null;
				}
			} while (answerFromShard.getTransaction().getTransactionIdDefinedByClient() != this.transactionId);
			
			if (answerFromShard.getTransaction().getTransactionIdDefinedByClient() == this.transactionId && answerFromShard.getMessageType() == Message.MessageType.READ_ANSWER) {
				positiveReadAnswers++;
				readOperationFromServer = answerFromShard.getTransaction().getReadSet().get(0);
				
				if (readOperationFromServer.getTimestamp() >= mostRecentTimestamp) {
					mostRecentTimestamp = readOperationFromServer.getTimestamp();
					bestReadOperationFromShards = readOperationFromServer;
				}
				
			} else if(answerFromShard.getTransaction().getTransactionIdDefinedByClient() == this.transactionId && answerFromShard.getMessageType() == Message.MessageType.READ_FAILED) {
				// Nothing to do here I guess?
			} else {
				// Invalid response from server (we screwed up when listening to messages)
				// That's bad!!!!
				LOGGER.info("------sendReadRequestToShards---Thread"
						+ Thread.currentThread().getId() + " ---Transaction Id "
						+ transactionId
						+ " --- GOT WRONG MESSAGE - PROBLEM! -- We got this type of message:" + answerFromShard.getMessageType() + " | txnID received: " + answerFromShard.getTransaction().getTransactionIdDefinedByClient());
				return null;
			}
		//}
		
		if (positiveReadAnswers >= this.acceptanceCriteria) {
			return bestReadOperationFromShards;
		}
		return null;
	}
	
	public void sendMessageToShard(Message message, String shardIpAddress) {
		// Send a message to a client
		InetAddress clientAddress;
				
		try {
			clientAddress = InetAddress.getByName(shardIpAddress);
			byte[] bytesToSend = message.serialize();
			
			DatagramPacket sendPacket = new DatagramPacket(bytesToSend, bytesToSend.length, 
					clientAddress, this.shardPort);
					
			this.socket.send(sendPacket);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public Message receiveMessageFromShards() {
		DatagramPacket packet;
		byte[] buffer;
		int sizePacket;
		
		buffer = new byte[BUFFER_SIZE];
		packet = new DatagramPacket(buffer, buffer.length);

		try {
			this.socket.receive(packet);
			
			byte[] receivedBytes;
			Message messageFromShard;
			receivedBytes = packet.getData();
			LOGGER.info("receive packet from :" + packet.getAddress() + " | sizeOfPacket:" + packet.getLength());
			messageFromShard = Message.deserialize(receivedBytes);
			return messageFromShard;
		} catch (SocketTimeoutException e) {
			return null;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public void convertReadValuesToYCSBformat(HashMap<String, String> readValues, HashMap<String, ByteIterator> ycsbReadValues) {
		StringByteIterator.putAllAsByteIterators(ycsbReadValues, readValues);
	}
	
	public void convertYCSBwriteValuesToInternalFormat(HashMap<String, ByteIterator> ycsbWriteValues, HashMap<String, String> writeValues) {
		StringByteIterator.putAllAsStrings(writeValues, ycsbWriteValues);
	}
	
	public int getCoordinatorShardID() {
		return (new Random()).nextInt(this.shardsPerDatacenter);
	}
}
