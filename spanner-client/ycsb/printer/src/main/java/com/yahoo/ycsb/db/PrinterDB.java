package com.yahoo.ycsb.db;

import static com.yahoo.ycsb.Status.NOT_IMPLEMENTED;
import static com.yahoo.ycsb.Status.OK;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Logger;

import com.yahoo.ycsb.*;


public class PrinterDB extends DB {

	private final static Logger LOGGER = Logger.getLogger(PrinterDB.class
			.getName());

	public PrinterDB() {

	}

	@Override
	public void init() throws DBException {

		LOGGER.info("\n--------------------------\nInit method is called ...\n------------------------\n");
	}

	@Override
	public void cleanup() throws DBException {
		LOGGER.info("\n--------------------------\nCleanUp method is called ...\n------------------------\n");
	}

	@Override
	public void start() throws DBException {
		LOGGER.info("\n--------------------------\n Start method is called ...\n------------------------\n");

	}

	@Override
	public void commit() throws DBException {

		LOGGER.info("\n--------------------------\nCommit method is called ...\n------------------------\n");

	}

	@Override
	public void abort() throws DBException {
		LOGGER.info("\n--------------------------\nAbort method is called ...\n------------------------\n");

	}

	@Override
	public Status read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		LOGGER.info("\n--------------------------\nRead method is called ...\n------------------------\n");
		System.out
				.println("\n--------------------------\nRead method is called ...\n------------------------\n");

		return Status.OK;
	}

	@Override
	public Status scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

		LOGGER.info("\n--------------------------\nScan method is called ...\n------------------------\n");

		return NOT_IMPLEMENTED;
	}

	@Override
	public Status update(String table, String key,
			HashMap<String, ByteIterator> values) {

		LOGGER.info("\n--------------------------\n Update method is called ...\n------------------------\n");

		return OK;
	}

	@Override
	public Status insert(String table, String key,
			HashMap<String, ByteIterator> values) {

		LOGGER.info("\n--------------------------\nInsert method is called ...\n------------------------\n");

		return OK;

	}

	@Override
	public Status delete(String table, String key) {

		LOGGER.info("\n--------------------------\nDelete method is called ...\n------------------------\n");

		return NOT_IMPLEMENTED;
	}

}
