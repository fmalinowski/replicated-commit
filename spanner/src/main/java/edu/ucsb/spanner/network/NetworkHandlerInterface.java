package edu.ucsb.spanner.network;

import edu.ucsb.spanner.Shard;
import edu.ucsb.spanner.model.Message;
import edu.ucsb.spanner.model.Transaction;


public interface NetworkHandlerInterface {

	public void sendMessageToShard(Shard shard, Message message);
	
	public void sendMessageToClient(Transaction transaction, Message message);
	
}
