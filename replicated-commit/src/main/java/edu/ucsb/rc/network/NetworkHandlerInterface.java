package edu.ucsb.rc.network;

import edu.ucsb.rc.Shard;
import edu.ucsb.rc.transactions.Transaction;

public interface NetworkHandlerInterface {

	public void sendMessageToShard(Shard shard, Message message);
	
	public void sendMessageToClient(Transaction transaction, Message message);
	
}
