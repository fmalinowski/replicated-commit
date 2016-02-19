package edu.ucsb.rc;

import java.io.FileInputStream;
import java.util.Properties;

public class ConfigReader {
	private Properties properties = null;
	
	public ConfigReader(String configFilePath) {		
		FileInputStream file;
		
		try {
			file = new FileInputStream(configFilePath);
			properties = new Properties();
			properties.load(file);
			file.close();
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
	
	public int getShardListeningPort() {
		if (this.properties == null) {
			return -1;
		}
		return  Integer.parseInt(properties.getProperty("shardListeningPort"));
	}
	
	public int getClientListeningPort() {
		if (this.properties == null) {
			return -1;
		}
		return  Integer.parseInt(properties.getProperty("clientListeningPort"));
	}
	
	public MultiDatacenter initializeMultiDatacenter() {
		if (this.properties == null) {
			return null;
		}
		
		int datacentersNumber, shardsPerDatacenter, currentDatacenterID, currentShardID;
		String shardIPAddress;
		MultiDatacenter multiDatacenter = null;
		Datacenter datacenter;
		Shard shard;
	
		datacentersNumber = Integer.parseInt(properties.getProperty("numberDatacenters"));
		shardsPerDatacenter = Integer.parseInt(properties.getProperty("numberShardsPerDatacenter"));
		currentDatacenterID = Integer.parseInt(properties.getProperty("currentDatacenter"));
		currentShardID = Integer.parseInt(properties.getProperty("currentShard"));
		
		multiDatacenter = MultiDatacenter.getInstance();
		
		for (int datacenterID = 0; datacenterID < datacentersNumber; datacenterID++) {
			datacenter = new Datacenter();
			datacenter.setDatacenterID(datacenterID);
			multiDatacenter.addDatacenter(datacenter);
			
			for (int shardID = 0; shardID < shardsPerDatacenter; shardID++) {
				shardIPAddress = properties.getProperty("DC" + datacenterID + "-Shard" + shardID);
				shard = new Shard();
				shard.setShardID(shardID);
				shard.setIpAddress(shardIPAddress);
				datacenter.addShard(shard);
			}
		}
		
		datacenter = multiDatacenter.getDatacenter(currentDatacenterID);
		shard = datacenter.getShard(currentShardID);
		multiDatacenter.setCurrentDatacenter(datacenter);
		multiDatacenter.setCurrentShard(shard);
	
		return multiDatacenter;
	}
}
