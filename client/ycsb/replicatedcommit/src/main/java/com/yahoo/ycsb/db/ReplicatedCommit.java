package com.yahoo.ycsb.db;

import static com.yahoo.ycsb.Status.ERROR;
import static com.yahoo.ycsb.Status.NOT_IMPLEMENTED;
import static com.yahoo.ycsb.Status.OK;
import static com.yahoo.ycsb.Status.SERVICE_UNAVAILABLE;
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
	private static final String CLASS_NAME = ReplicatedCommit.class.getName();

	private ArrayList<Operation> currentWriteSet;
	private Random randomGenerator;
	private HashMap<String, ByteIterator> readResult;
	private NetworkUtils networkUtils;

	public ReplicatedCommit() {

		LOGGER = Logger.getLogger(CLASS_NAME);
		randomGenerator = new Random();
		networkUtils = new NetworkUtils(DATA_CENTER_SIZE);

	}

	@Override
	public void init() throws DBException {

		transactionId = randomGenerator.nextLong();

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

		readResult = null;
		currentTransaction = new Transaction();
		currentWriteSet = new ArrayList<Operation>();
		currentTransaction.setTransactionIdDefinedByClient(transactionId);

		transactionId++;
	}

	@Override
	public Status read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {

		Status returnValue = ERROR;
		
		LOGGER.info("------Read method---Thread "
				+ Thread.currentThread().getId() + " ---Transaction Id "
				+ transactionId + " \n Read Key= " + key + " Fields size = "
				+ fields.size());

		// Send Read Request
		Message message = createReadRequestMessage(key, fields);
		networkUtils.sendMessageToCoordinators(message);

		// Receive Response
		List<Message> messagesReceived = networkUtils.receiveFromCoordinators();

		if (areMessagesNotEmpty(messagesReceived)) {
			
			LOGGER.info("------Read was sucessful ---Thread "
					+ Thread.currentThread().getId() + " ---Transaction Id "
					+ transactionId + "Current Write Set "
					+ currentWriteSet.size());
			
			// Read value should be the latest timestamp
			// Update the read set in the current transaction

			// ArrayList<Operation> currentReadSet =
			// currentTransaction.getReadSet();
			// if(currentReadSet ==null)
			// currentReadSet = new ArrayList<Operation>();

			// currentReadSet.add(readOperation);
			// readOperation.setColumnValues(columnValues);
			// currentTransaction.setReadSet(currentReadSet);
			
			returnValue = OK;
		} else {
			
			LOGGER.info("------Read failed---Thread "
					+ Thread.currentThread().getId() + " ---Transaction Id "
					+ transactionId + "Current Write Set "
					+ currentWriteSet.size());
			
			returnValue = SERVICE_UNAVAILABLE;
		}

		return returnValue;
	}

	private Message createReadRequestMessage(String key, Set<String> fields) {

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

	@Override
	public void commit() throws DBException {

		if (currentTransaction != null) {

			currentTransaction.setWriteSet(currentWriteSet);
			Message message = new Message();
			message.setMessageType(PAXOS__ACCEPT_REQUEST);
			message.setTransaction(currentTransaction);

			networkUtils.sendMessageToCoordinators(message);

			List<Message> messagesReceived = networkUtils
					.receiveFromCoordinators();

			// What should you do if the commit was a failure?
			if (areMessagesNotEmpty(messagesReceived)) {

				processCommitResponse(messagesReceived);

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
		} else {
			LOGGER.info("There were no operations in the transaction to commit!! ");
		}

	}

	private boolean areMessagesNotEmpty(List<Message> messagesReceived) {
		return messagesReceived != null && !messagesReceived.isEmpty();
	}

	private void processCommitResponse(List<Message> messagesReceived) {
		// TODO Auto-generated method stub

	}

	@Override
	public void abort() throws DBException {

		LOGGER.info("------Abort method---Thread "
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
