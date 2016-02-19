package edu.ucsb.rc;

import java.util.logging.Logger;

import edu.ucsb.rc.network.NetworkHandler;

public class App {
	private final static Logger LOGGER = Logger.getLogger(App.class.getName()); 
	private final static String configFilePath = "./config.properties";
	
    public static void main(String[] args) {
    	ConfigReader configReader;
        MultiDatacenter multiDatacenter;
        NetworkHandler networkHandler;
        int shardListeningPort, clientListeningPort;
        
        configReader = new ConfigReader(configFilePath);
        shardListeningPort = configReader.getShardListeningPort();
        clientListeningPort = configReader.getClientListeningPort();
        multiDatacenter = configReader.initializeMultiDatacenter();
        
        networkHandler = new NetworkHandler(multiDatacenter, clientListeningPort, shardListeningPort);
        
        LOGGER.info("Listening for shard messages");
        networkHandler.listenForShards();
        LOGGER.info("Listening for client messages");
        networkHandler.listenForClients();
        
        while (true);
	}
}
