package com.yahoo.ycsb.db;

import static com.yahoo.ycsb.Status.ERROR;
import static com.yahoo.ycsb.Status.NOT_IMPLEMENTED;
import static com.yahoo.ycsb.Status.OK;
import static com.yahoo.ycsb.Status.SERVICE_UNAVAILABLE;
import static com.yahoo.ycsb.Status.UNEXPECTED_STATE;
import static edu.ucsb.rc.model.Message.MessageType.PAXOS__ACCEPT_REQUEST;
import static edu.ucsb.rc.model.Message.MessageType.PAXOS__ACCEPT_REQUEST_ACCEPTED;
import static edu.ucsb.rc.model.Message.MessageType.READ_ANSWER;
import static edu.ucsb.rc.model.Message.MessageType.READ_REQUEST;
import static edu.ucsb.rc.model.Operation.Type.READ;
import static edu.ucsb.rc.model.Operation.Type.WRITE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
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

			if (messagesReceived.size() <= 3) {

				if (isMaximumQuorum(messagesReceived, READ_ANSWER)) {

					result = extractResult(messagesReceived);
					returnValue = OK;

				} else {
					returnValue = ERROR;
				}

			} else {

				LOGGER.info("Too many messages received at the client end!");
				returnValue = UNEXPECTED_STATE;
			}

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

	private boolean isMaximumQuorum(List<Message> messagesReceived,
			MessageType messageType) {

		int count = 0;
		for (Message message : messagesReceived) {
			if (messageType == message.getMessageType()) {
				count++;
			}
		}

		if (count >= ACCEPTANCE_CRITERIA) {
			return true;
		}
		return false;
	}

	private HashMap<String, ByteIterator> extractResult(
			List<Message> messagesReceived) {

		HashMap<String, ByteIterator> byteMap = new HashMap<String, ByteIterator>();
		ArrayList<Operation> finalReadSet = new ArrayList<Operation>();
		long latestTimestamp = 0;

		// TODO Refactor this method

		for (Message receivedMessage : messagesReceived) {

			Transaction transaction = receivedMessage.getTransaction();
			if (transaction != null) {

				ArrayList<Operation> readSet = transaction.getReadSet();

				if (readSet != null) {
					for (Operation operation : readSet) {

						long timestamp = operation.getTimestamp();

						if (timestamp > latestTimestamp
								&& operation.getColumnValues() != null) {
							latestTimestamp = timestamp;

							HashMap<String, String> columnMap = operation
									.getColumnValues();

							Set<Entry<String, String>> columnMapEntries = columnMap
									.entrySet();
							for (Entry<String, String> entry : columnMapEntries) {
								// Overwriting
								byteMap.put(entry.getKey(),
										new ByteArrayByteIterator(entry
												.getValue().getBytes()));
							}

							// Repeated #op number of times
							finalReadSet = readSet;
						}

					}
				} else {
					LOGGER.info("Received an empty Read Set");
				}
			} else {
				LOGGER.info("Received dummy transaction!!");
			}
		}

		// Why do we need this again?
		updateCurrentReadSet(finalReadSet);

		return byteMap;
	}

	private void updateCurrentReadSet(ArrayList<Operation> finalReadSet) {

		ArrayList<Operation> currentReadSet = currentTransaction.getReadSet();
		if (currentReadSet == null) {
			currentReadSet = finalReadSet;
		} else {
			currentReadSet.addAll(finalReadSet);
		}

		// Dont think this statement is needed
		currentTransaction.setReadSet(currentReadSet);
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
			// Values are stored by the server
			maps.put(field, "");
		}

		return maps;
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
			if (areMessagesNotEmpty(messagesReceived)
					&& isMaximumQuorum(messagesReceived,
							PAXOS__ACCEPT_REQUEST_ACCEPTED)) {

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
		writeOperation
				.setColumnValues(convertByteIteratorMapToStringMap(values));

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
		writeOperation
				.setColumnValues(convertByteIteratorMapToStringMap(values));

		currentWriteSet.add(writeOperation);

		return OK;

	}

	private HashMap<String, String> convertByteIteratorMapToStringMap(
			HashMap<String, ByteIterator> byteMap) {
		if (byteMap == null)
			return null;

		HashMap<String, String> returnMap = new HashMap<String, String>(
				byteMap.size());
		Set<Entry<String, ByteIterator>> entrySet = byteMap.entrySet();
		for (Entry<String, ByteIterator> entry : entrySet) {
			returnMap.put(entry.getKey(), entry.getValue().toString());
		}

		return returnMap;
	}

	@Override
	public Status delete(String table, String key) {

		LOGGER.info("------Delete method---Thread"
				+ Thread.currentThread().getId() + " ---Transaction Id "
				+ transactionId);
		return NOT_IMPLEMENTED;
	}
}
