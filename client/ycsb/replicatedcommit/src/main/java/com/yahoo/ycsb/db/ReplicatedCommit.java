package com.yahoo.ycsb.db;

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
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Logger;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

import edu.ucsb.rc.model.Message;
import edu.ucsb.rc.model.Message.MessageType;
import edu.ucsb.rc.model.Operation;
import edu.ucsb.rc.model.Transaction;

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
	private HashMap<String, ByteIterator> readResult;

	public ReplicatedCommit() {

		ipMap = new HashMap<String, String>(DATA_CENTER_SIZE);
		portMap = new HashMap<String, Integer>(DATA_CENTER_SIZE);

		LOGGER = Logger.getLogger(CLASS_NAME);
		randomGenerator = new Random();

		String configFilePath = "config.properties";
		setPropertiesFromConfigFile(configFilePath);

	}

	@Override
	public void init() throws DBException {

		transactionId = randomGenerator.nextLong();

		try {
			ycsbSocket = new DatagramSocket();

		} catch (SocketException e) {
			e.printStackTrace();
		}

		LOGGER.info("------Init method---Thread "
				+ Thread.currentThread().getId() + " ---Transaction Id "
				+ transactionId);

	}

	@Override
	public void cleanup() throws DBException {
		LOGGER.info("------CleanUp method---Thread "
				+ Thread.currentThread().getId() + " ---Transaction Id "
				+ transactionId);
	}

	@Override
	public void start() throws DBException {

		LOGGER.info("------Start method---Thread "
				+ Thread.currentThread().getId() + " ---Transaction Id "
				+ transactionId);
		currentTransaction = new Transaction();
		currentWriteSet = new ArrayList<Operation>();
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

			boolean status = receive(PAXOS__ACCEPT_REQUEST_ACCEPTED, "COMMIT");

			// What should you do if the commit was a failure?
			if (status) {
				LOGGER.info("------Commit was sucessful ---Thread "
						+ Thread.currentThread().getId()
						+ " ---Transaction Id " + transactionId
						+ "Current Write Set " + currentWriteSet.size());
			} else {
				LOGGER.info("------Commit failed---Thread "
						+ Thread.currentThread().getId()
						+ " ---Transaction Id " + transactionId
						+ "Current Write Set " + currentWriteSet.size());
			}
		}

	}

	@Override
	public void abort() throws DBException {

		LOGGER.info("------Abort method---Thread "
				+ Thread.currentThread().getId() + " ---Transaction Id "
				+ transactionId);

	}

	@Override
	public Status read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {

		readResult = null;
		
		LOGGER.info("------Read method---Thread "
				+ Thread.currentThread().getId() + " ---Transaction Id "
				+ transactionId + " \n Read Key= " + key + " Fields size = "
				+ fields.size());

		Message message = createReadMessage(key, fields);

		send(message);
		boolean status = receive(READ_ANSWER, "READ");

		if (status) {
			LOGGER.info("------Read was sucessful ---Thread "
					+ Thread.currentThread().getId() + " ---Transaction Id "
					+ transactionId + "Current Write Set "
					+ currentWriteSet.size());
		} else {
			LOGGER.info("------Commit failed---Thread "
					+ Thread.currentThread().getId() + " ---Transaction Id "
					+ transactionId + "Current Write Set "
					+ currentWriteSet.size());
		}

		// Read value should be the latest timestamp
		// Update the read set in the current transaction

		// ArrayList<Operation> currentReadSet =
		// currentTransaction.getReadSet();
		// if(currentReadSet ==null)
		// currentReadSet = new ArrayList<Operation>();

		// currentReadSet.add(readOperation);
		// readOperation.setColumnValues(columnValues);
		// currentTransaction.setReadSet(currentReadSet);

		return OK;
	}

	private Message createReadMessage(String key, Set<String> fields) {

		Message message = new Message();
		Operation readOperation = new Operation();
		ArrayList<Operation> readSet = new ArrayList<Operation>();
		Transaction readTransaction = new Transaction();

		message.setMessageType(READ_REQUEST);

		readOperation.setKey(key);
		readOperation.setColumnValues(getHashMapOfFields(fields));
		readOperation.setType(READ);

		readSet.add(readOperation);
		readTransaction.setReadSet(readSet);

		message.setTransaction(readTransaction);

		return message;
	}

	private HashMap<String, String> getHashMapOfFields(Set<String> fields) {

		HashMap<String, String> maps = new HashMap<String, String>(
				fields.size());

		for (String field : fields) {
			maps.put(field, "");
		}

		return maps;
	}

	private boolean receive(MessageType expectedMessageType, String mode) {

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
				byte[] receivedBytes = packet.getData();
				Message messageFromCoordinators = Message
						.deserialize(receivedBytes);

				totalAcceptedResponses += processMessage(
						messageFromCoordinators, expectedMessageType);
				if ("READ".equals(mode)) {
					processReadMessage(messageFromCoordinators);
				}
				counter--;

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return (totalAcceptedResponses >= ACCEPTANCE_CRITERIA) ? true : false;

	}

	private HashMap<String, ByteIterator> processReadMessage(Message message) {

		Transaction transaction = message.getTransaction();
		HashMap<String, ByteIterator> byteMap = new HashMap<String, ByteIterator>();

		if (transaction != null) {
			ArrayList<Operation> readSet = transaction.getReadSet();
			if (readSet != null) {
				for (Operation operation : readSet) {

					String key = operation.getKey();
					long timestamp = operation.getTimestamp();
					HashMap<String, String> columnValues = operation
							.getColumnValues();

					Set<Entry<String, String>> stringMap = columnValues
							.entrySet();
					for (Entry<String, String> entry : stringMap) {
						byteMap.put(entry.getKey(), new ByteArrayByteIterator(
								entry.getValue().getBytes()));
					}

				}
			}
		}
		return byteMap;

	}

	private int processMessage(Message messageFromCoordinators,
			MessageType expectedMessageType) {

		MessageType messageType = messageFromCoordinators != null ? messageFromCoordinators
				.getMessageType() : READ_FAILED;
		int returnValue = 0;
		if (messageType == expectedMessageType) {

			returnValue = 1;
		}

		LOGGER.info("------Received " + messageType + "  ---Thread "
				+ Thread.currentThread().getId() + " ---Transaction Id "
				+ transactionId);

		return returnValue;
	}

	private void send(Message message) {

		String[] ipAddresses = getCoordinatorIPAddresses(properties);
		int[] portNumbers = getCoordinatorPorts(properties);

		for (int i = 0; i < DATA_CENTER_SIZE; i++) {
			sendMessageToCoordinator(ipAddresses[i], portNumbers[i], message);
		}

	}

	private void setPropertiesFromConfigFile(String configFilePath) {

		Properties properties = new Properties();

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
			DatagramSocket socket = new DatagramSocket();
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
