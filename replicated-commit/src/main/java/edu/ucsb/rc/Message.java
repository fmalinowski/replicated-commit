package edu.ucsb.rc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import edu.ucsb.rc.transactions.Transaction;

public class Message implements Serializable {
	private static final long serialVersionUID = -2505050279340559507L;
	
	public enum MessageType {
		READ_REQUEST,
		PAXOS__ACCEPT_REQUEST,
		TWO_PHASE_COMMIT__PREPARE,
		TWO_PHASE_COMMIT__PREPARE_ACCEPETD,
		TWO_PHASE_COMMIT__PREPARE_DENIED,
		PAXOS__ACCEPT_REQUEST_ACCEPTED,
		TWO_PHASE_COMMIT__COMMIT		
	}
	
	private MessageType messageType;
	private Transaction transaction;
	
	// We will use this class to send messages over the network for clients and shards
	// All the objects that are contained in this class and that we want to send over the network
	// needs to implements Serializable !
	
	public byte[] serialize() {
		ByteArrayOutputStream bos;
		ObjectOutput out;
		
		bos = new ByteArrayOutputStream();
		out = null;
		
		try {
			out = new ObjectOutputStream(bos);
			out.writeObject(this);
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	    return bos.toByteArray();
	}
	
	public static Message deserialize(byte[] receivedBytes) {
		ObjectInputStream iStream;
		Message receivedMessage;
		
		receivedMessage = null;
		
		try {
			iStream = new ObjectInputStream(new ByteArrayInputStream(receivedBytes));
			receivedMessage = (Message) iStream.readObject();
			iStream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}		
		
		return receivedMessage;
	}

	public MessageType getMessageType() {
		return messageType;
	}

	public void setMessageType(MessageType messageType) {
		this.messageType = messageType;
	}

	public Transaction getTransaction() {
		return transaction;
	}

	public void setTransaction(Transaction transaction) {
		this.transaction = transaction;
	}
}
