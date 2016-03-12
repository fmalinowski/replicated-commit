package edu.ucsb.rc;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import edu.ucsb.rc.model.Operation;
import edu.ucsb.rc.model.Transaction;

public class CommitLogger {
	File logFile;
	FileWriter fileWriter;
	
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
		
		try {
			this.fileWriter = new FileWriter(this.logFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void logCommit(Transaction t) {
		String commitLine;
		ArrayList<Operation> writeSet = t.getWriteSet();
		
		commitLine = "Commit txn:" + t.getServerTransactionId() + " ||| Writing objects: ";
		
		for (Operation writeOp : writeSet) {
			commitLine += writeOp.getKey();
			
			if (writeOp.getShardIdHoldingData() == MultiDatacenter.getInstance().getCurrentShard().getShardID()) {
				commitLine += " (cs)";
			}
			
			commitLine += " | ";
		}
		commitLine += "\n";
		
		try {
			this.fileWriter.write(commitLine);
			this.fileWriter.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void closeLog() {
		try {
			this.fileWriter.close();
		} catch (Exception e) {
		}
	}

}
