package edu.ucsb.rc;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
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
        
        // Provide command line parameter -DactivateLog=true to enable logging
        activateLoggingIfNeeded();
        
        configReader = new ConfigReader(configFilePath);
        shardListeningPort = configReader.getShardListeningPort();
        clientListeningPort = configReader.getClientListeningPort();
        multiDatacenter = configReader.initializeMultiDatacenter();
        multiDatacenter.initializeShards();
        multiDatacenter.initializeDatastore();
        
        networkHandler = new NetworkHandler(multiDatacenter, clientListeningPort, shardListeningPort);
        multiDatacenter.setNetworkHandler(networkHandler);
        
        LOGGER.info("Listening for shard messages");
        networkHandler.listenForShards();
        LOGGER.info("Listening for client messages");
        networkHandler.listenForClients();
        
        while (true);
	}
    
    public static void activateLoggingIfNeeded() {
    	boolean activateLogging = Boolean.parseBoolean(System.getProperty("activateLog"));
        Logger log = LogManager.getLogManager().getLogger("");
        for (Handler h : log.getHandlers()) {
        	if (activateLogging) {
        		h.setLevel(Level.INFO);
        	} else {
        		h.setLevel(Level.OFF);
        	}
        	
        }
    }
}
