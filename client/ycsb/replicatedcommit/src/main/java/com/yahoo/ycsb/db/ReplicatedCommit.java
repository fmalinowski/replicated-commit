package com.yahoo.ycsb.db;

import static com.yahoo.ycsb.Status.ERROR;
import static com.yahoo.ycsb.Status.NOT_IMPLEMENTED;
import static com.yahoo.ycsb.Status.OK;
import static edu.ucsb.rc.model.Message.MessageType.PAXOS__ACCEPT_REQUEST;
import static edu.ucsb.rc.model.Message.MessageType.PAXOS__ACCEPT_REQUEST_ACCEPTED;
import static edu.ucsb.rc.model.Message.MessageType.READ_ANSWER;
import static edu.ucsb.rc.model.Message.MessageType.READ_FAILED;
import static edu.ucsb.rc.model.Message.MessageType.READ_REQUEST;
import static edu.ucsb.rc.model.Operation.Type.READ;
import static edu.ucsb.rc.model.Operation.Type.WRITE;

import java.io.FileInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Logger;

import com.yahoo.ycsb.*;
import edu.ucsb.rc.model.*;
import edu.ucsb.rc.model.Message.MessageType;

public class ReplicatedCommit extends DB {

	private Transaction currentTransaction;
	private Long transactionId;

	private final Logger LOGGER;

	private static final int DATA_CENTER_SIZE = 3;
	private static final int ACCEPTANCE_CRITERIA = 2;
	private static final int BUFFER_SIZE = 65507;
	private static final String CLASS_NAME = ReplicatedCommit.class.getName();
	private static final boolean shouldStopReceiver = false;

	private ArrayList<Operation> currentWriteSet;
	private Properties properties;
	private DatagramSocket ycsbSocket;
	private Random randomGenerator;

	private HashMap<String, String> ipMap;
	private HashMap<String, Integer> portMap;

	public ReplicatedCommit() {

		ipMap = new HashMap<String, String>(DATA_CENTER_SIZE);
		portMap = new HashMap<String, Integer>(DATA_CENTER_SIZE);

		LOGGER = Logger.getLogger(CLASS_NAME);
		randomGenerator = new Random();
		String configFilePath = "config.properties";

		try {
			FileInputStream file = new FileInputStream(configFilePath);
			properties = new Properties();
			properties.load(file);
			file.close();
		} catch (Exception e) {
			e.printStackTrace();
		}	

		portMap.put("Port1",
				Integer.parseInt(properties.getProperty("DC0-Shard1Port")));
		portMap.put("Port2",
				Integer.parseInt(properties.getProperty("DC1-Shard1Port")));
		portMap.put("Port3",
				Integer.parseInt(properties.getProperty("DC2-Shard1Port")));
		ipMap.put("IP1", properties.getProperty("DC0-Shard1"));
		ipMap.put("IP2", properties.getProperty("DC1-Shard1"));
		ipMap.put("IP3", properties.getProperty("DC2-Shard1"));

	}

	@Override
	public void init() throws DBException {

		transactionId = randomGenerator.nextLong();
		currentWriteSet = new ArrayList<Operation>();

		try {
			ycsbSocket = new DatagramSocket();

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

		LOGGER.info("------Start method---Thread"
				+ Thread.currentThread().getId() + " ---Transaction Id "
				+ transactionId);
		currentTransaction = new Transaction();
		currentTransaction.setTransactionIdDefinedByClient(transactionId);
		transactionId++;
	}

	@Override
	public void commit() throws DBException {

		if (currentTransaction != null) {

			currentTransaction.setWriteSet(currentWriteSet);
			Message message = new Message();
			message.setMessageType(PAXOS__ACCEPT_REQUEST);
			message.setTransaction(currentTransaction);

			send(message);

			// boolean status = receive();
			// if (status) {
			// LOGGER.info("Commit was successful");
			// }
		}

		LOGGER.info("------Commit method---Thread"
				+ Thread.currentThread().getId() + " ---Transaction Id "
				+ transactionId + "Current Write Set " + currentWriteSet.size());

	}

	@Override
	public void abort() throws DBException {

		LOGGER.info("------Abort method---Thread"
				+ Thread.currentThread().getId() + " ---Transaction Id "
				+ transactionId);

	}

	@Override
	public Status read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {

		LOGGER.info("------Read method---Thread"
				+ Thread.currentThread().getId() + " ---Transaction Id "
				+ transactionId);

		Message message = new Message();
		Operation readOperation = new Operation();
		ArrayList<Operation> readSet = new ArrayList<Operation>();
		Transaction readTransaction = new Transaction();

		message.setMessageType(READ_REQUEST);
		readOperation.setKey(key);
		// Assume only full reads
		// readOperation.setColumnValues(columnValues);
		readOperation.setType(READ);
		readSet.add(readOperation);
		readTransaction.setReadSet(readSet);
		message.setTransaction(readTransaction);

		// Also look for read timestamps/versions
		send(message);

		// Commented on March 03
		/*
		 * receive();
		 * 
		 * Result r = null; for (KeyValue kv : r.raw()) {
		 * 
		 * result.put(Bytes.toString(kv.getQualifier()), new
		 * ByteArrayByteIterator(kv.getValue())); }
		 */
		// Read value should be the latest timestamp
		// Update the read set in the current transaction
		


		//ArrayList<Operation> currentReadSet = currentTransaction.getReadSet();
		//if(currentReadSet ==null)
			//currentReadSet = new ArrayList<Operation>();

		//currentReadSet.add(readOperation);
		// readOperation.setColumnValues(columnValues);
		//currentTransaction.setReadSet(currentReadSet);

		return OK;
	}

	private boolean receive() {
		int counter = DATA_CENTER_SIZE;
		int totalAcceptedResponses = 0;
		while (counter > 0) {
			DatagramPacket packet;
			byte[] buffer;

			if (shouldStopReceiver) {
				return false;
			}

			buffer = new byte[BUFFER_SIZE];
			packet = new DatagramPacket(buffer, buffer.length);

			try {
				ycsbSocket.receive(packet);
				totalAcceptedResponses += processPacket(packet);
				counter--;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return (totalAcceptedResponses >= ACCEPTANCE_CRITERIA) ? true : false;

	}

	private int processPacket(DatagramPacket packet) {

		byte[] receivedBytes = packet.getData();
		Message messageFromCoordinators = Message.deserialize(receivedBytes);
		MessageType messageType = messageFromCoordinators != null ? messageFromCoordinators
				.getMessageType() : READ_FAILED;
		if (messageType == PAXOS__ACCEPT_REQUEST_ACCEPTED
				|| messageType == READ_ANSWER) {
			return 1;
		}

		return 0;
	}

	private void send(Message message) {

		String[] ipAddresses = getCoordinatorIPAddresses(properties);
		int[] portNumbers = getCoordinatorPorts(properties);

		for (int i = 0; i < DATA_CENTER_SIZE; i++) {
			sendMessageToCoordinator(ipAddresses[i], portNumbers[i], message);
		}

	}

	private Properties getCoordinatorProperties(String configFilePath) {
		FileInputStream file;
		Properties properties = new Properties();
		try {
			file = new FileInputStream(configFilePath);

			properties.load(file);
			file.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return properties;
	}

	private int[] getCoordinatorPorts(Properties properties) {

		int[] ports = new int[DATA_CENTER_SIZE];

		ports[0] = portMap.get("Port1");
		ports[1] = portMap.get("Port2");
		ports[2] = portMap.get("Port3");

		return ports;
	}

	private String[] getCoordinatorIPAddresses(Properties properties) {

		String[] ipAddresses = new String[DATA_CENTER_SIZE];
		ipAddresses[0] = ipMap.get("IP1");
		ipAddresses[1] = ipMap.get("IP2");
		ipAddresses[2] = ipMap.get("IP3");

		return ipAddresses;
	}

	private void sendMessageToCoordinator(String ipAddress, int portNumber,
			Message message) {

		try {
			DatagramSocket socket = new DatagramSocket(portNumber);
			InetAddress clientAddress = InetAddress.getByName(ipAddress);
			byte[] bytesToSend = message.serialize();

			DatagramPacket sendPacket = new DatagramPacket(bytesToSend,
					bytesToSend.length, clientAddress, portNumber);

			socket.send(sendPacket);
			LOGGER.info("Packet to " + ipAddress + ":" + portNumber + " sent.");
			socket.close();
		} catch (Exception e) {

			LOGGER.info("Sending message to the coordinators failed! ");
			e.printStackTrace();
		}
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
	public Status update(String table, String key,
			HashMap<String, ByteIterator> values) {

		LOGGER.info("------Update method---Thread"
				+ Thread.currentThread().getId() + " ---Transaction Id "
				+ transactionId);

		Operation writeOperation = new Operation();
		writeOperation.setKey(key);
		writeOperation.setType(WRITE);
		// Convert the Byte Iterator to String
		// writeOperation.setColumnValues(values);
		currentWriteSet.add(writeOperation);

		return OK;
	}

	@Override
	public Status insert(String table, String key,
			HashMap<String, ByteIterator> values) {

		LOGGER.info("------Insert method---Thread"
				+ Thread.currentThread().getId() + " ---Transaction Id "
				+ transactionId);

		Operation writeOperation = new Operation();
		writeOperation.setKey(key);
		writeOperation.setType(WRITE);
		// Convert the Byte Iterator to String
		// writeOperation.setColumnValues(values);
		currentWriteSet.add(writeOperation);

		return OK;

	}

	@Override
	public Status delete(String table, String key) {

		LOGGER.info("------Delete method---Thread"
				+ Thread.currentThread().getId() + " ---Transaction Id "
				+ transactionId);
		return NOT_IMPLEMENTED;
	}
}
