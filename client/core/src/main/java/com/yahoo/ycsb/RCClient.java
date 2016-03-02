package com.yahoo.ycsb;

import static com.yahoo.ycsb.Status.NOT_IMPLEMENTED;
import static com.yahoo.ycsb.Status.OK;
import static edu.ucsb.rc.network.Message.MessageType.PAXOS__ACCEPT_REQUEST;
import static edu.ucsb.rc.network.Message.MessageType.PAXOS__ACCEPT_REQUEST_ACCEPTED;
import static edu.ucsb.rc.network.Message.MessageType.READ_ANSWER;
import static edu.ucsb.rc.network.Message.MessageType.READ_FAILED;
import static edu.ucsb.rc.transactions.Operation.Type.WRITE;

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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import edu.ucsb.rc.network.Message;
import edu.ucsb.rc.network.Message.MessageType;
import edu.ucsb.rc.transactions.Operation;
import edu.ucsb.rc.transactions.Operation.Type;
import edu.ucsb.rc.transactions.Transaction;

public class RCClient extends DB {

	private Transaction currentTransaction;
	private Long transactionId;
	private final static Logger LOGGER = Logger.getLogger(RCClient.class
			.getName());
	private static final int DATA_CENTER_SIZE = 3;
	private static final int ACCEPTANCE_CRITERIA = 2;
	private static final int BUFFER_SIZE = 65507;
	private static boolean shouldStopReceiver = false;

	private ArrayList<Operation> currentWriteSet;
	private Properties properties;
	private DatagramSocket ycsbSocket;

	public RCClient() {

	}

	@Override
	public void init() throws DBException {
		Random randomGenerator = new Random();
		transactionId = randomGenerator.nextLong();
		currentWriteSet = new ArrayList<Operation>();
		properties = new Properties();
		properties = getCoordinatorProperties("");

		try {
			ycsbSocket = new DatagramSocket();

		} catch (SocketException e) {

			e.printStackTrace();
		}

	}

	@Override
	public void cleanup() throws DBException {
		LOGGER.info("The unimplemented cleanup method was called");
	}

	@Override
	public void start() throws DBException {
		currentTransaction = new Transaction();
		currentTransaction.setTransactionIdDefinedByClient(transactionId);
		transactionId++;
	}

	@Override
	public void commit() throws DBException {

		if (currentTransaction != null) {
			Message message = new Message();
			message.setMessageType(PAXOS__ACCEPT_REQUEST);
			message.setTransaction(currentTransaction);

			send(message);
			boolean status = receive();
			if (status) {
				LOGGER.info("Commit was successful");
			}
		}
		LOGGER.info("Commit failed!");

	}

	@Override
	public void abort() throws DBException {
		LOGGER.info("Trying to abort the transaction");
	}

	@Override
	public Status read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {

		Message message = new Message();
		message.setMessageType(MessageType.READ_REQUEST);
		Operation readOperation = new Operation();
		readOperation.setKey(key);
		// Assume only full reads
		// readOperation.setColumnValues(columnValues);

		readOperation.setType(Type.READ);
		ArrayList<Operation> readSet = new ArrayList<Operation>();
		readSet.add(readOperation);
		Transaction readTransaction = new Transaction();
		readTransaction.setReadSet(readSet);
		message.setTransaction(readTransaction);

		// Also look for read timestamps/versions
		send(message);

		receive();

		Result r = null;
		for (KeyValue kv : r.raw()) {

			result.put(Bytes.toString(kv.getQualifier()),
					new ByteArrayByteIterator(kv.getValue()));
		}

		// Read value should be the latest timestamp
		// Update the read set in the current transaction
		ArrayList<Operation> currentReadSet = currentTransaction.getReadSet();
		currentReadSet.add(readOperation);
		// readOperation.setColumnValues(columnValues);
		currentTransaction.setReadSet(currentReadSet);

		return Status.ERROR;
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
		ports[0] = Integer.parseInt(properties.getProperty("coordinator0Port"));
		ports[1] = Integer.parseInt(properties.getProperty("coordinator1Port"));
		ports[2] = Integer.parseInt(properties.getProperty("coordinator2Port"));

		return ports;
	}

	private String[] getCoordinatorIPAddresses(Properties properties) {

		String[] ipAddresses = new String[DATA_CENTER_SIZE];
		ipAddresses[0] = properties.getProperty("coordinator0IP");
		ipAddresses[1] = properties.getProperty("coordinator0IP");
		ipAddresses[2] = properties.getProperty("coordinator0IP");

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

		LOGGER.info("The unimplemented scan method was called");
		return NOT_IMPLEMENTED;
	}

	@Override
	public Status update(String table, String key,
			HashMap<String, ByteIterator> values) {

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

		LOGGER.info("The unimplemented delete method was called");
		return NOT_IMPLEMENTED;
	}

}
