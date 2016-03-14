package edu.ucsb.rc;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import edu.ucsb.rc.model.Operation;
import edu.ucsb.rc.model.Transaction;

public class CommitLogger {
	File logFile;
	
	public CommitLogger() {
		this.logFile = new File("commitLog.txt");
		
		if (!this.logFile.exists()) {
			try {
				this.logFile.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			this.logFile.delete();
			try {
				this.logFile.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void logCommit(Transaction t) {
		String commitLine;
		ArrayList<Operation> writeSet = t.getWriteSet();
		int shardIdHoldingData;
		
		commitLine = "Commit txn:" + t.getServerTransactionId() + " ||| Writing objects: ";
		
		for (Operation writeOp : writeSet) {
			commitLine += writeOp.getKey();
			
			shardIdHoldingData = MultiDatacenter.getInstance().getCurrentDatacenter().getShardIdForKey(writeOp.getKey());
			if (shardIdHoldingData == MultiDatacenter.getInstance().getCurrentShard().getShardID()) {
				commitLine += " (cs)";
			}
			
			commitLine += " | ";
		}
		commitLine += "\n";
		
		FileWriter fileWriter;
		try {
			synchronized(this) {
				fileWriter = new FileWriter(this.logFile, true);
				fileWriter.write(commitLine);
				fileWriter.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
