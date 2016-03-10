package com.yahoo.ycsb.db;

import java.io.FileInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import edu.ucsb.rc.model.Message;

public class NetworkUtils {

	private Properties properties;
	private DatagramSocket ycsbSocket;
	private HashMap<String, String> ipMap;
	private HashMap<String, Integer> portMap;

	private static final String CLASS_NAME = NetworkUtils.class.getName();
	private static final int BUFFER_SIZE = 65507;
	private static final String configFilePath = "config.properties";

	private final Logger LOGGER;
	private final int numberOfDatacenters;

	public NetworkUtils(int numberOfDatacenters) {

		this.numberOfDatacenters = numberOfDatacenters;
		ipMap = new HashMap<String, String>(numberOfDatacenters);
		portMap = new HashMap<String, Integer>(numberOfDatacenters);

		LOGGER = Logger.getLogger(CLASS_NAME);
		setPropertiesFromConfigFile(configFilePath);
	}

	public void sendMessageToCoordinators(Message message) {

		String[] ipAddresses = getCoordinatorIPAddresses(properties);
		int[] portNumbers = getCoordinatorPorts(properties);

		for (int i = 0; i < numberOfDatacenters; i++) {
			sendMessageToCoordinator(ipAddresses[i], portNumbers[i], message);
		}

	}

	private void sendMessageToCoordinator(String ipAddress, int portNumber,
			Message message) {

		try {
			DatagramSocket socket = new DatagramSocket();
			InetAddress clientAddress = InetAddress.getByName(ipAddress);
			byte[] bytesToSend = message.serialize();

			DatagramPacket sendPacket = new DatagramPacket(bytesToSend,
					bytesToSend.length, clientAddress, portNumber);

			socket.send(sendPacket);
			LOGGER.info("Packet to " + ipAddress + ":" + portNumber + " sent.");
			socket.close();
		} catch (Exception e) {

			LOGGER.info("Sending message to the coordinators failed! ");
			e.printStackTrace();
		}
	}

	public List<Message> receiveFromCoordinators() {

		int counter = numberOfDatacenters;
		List<Message> messagesReceived = new ArrayList<Message>();
		
		//Implement a timeout if necessary

		while (counter > 0) {

			byte[] buffer = new byte[BUFFER_SIZE];
			DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

			try {

				ycsbSocket.receive(packet);
				byte[] receivedBytes = packet.getData();
				messagesReceived.add(Message.deserialize(receivedBytes));

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return messagesReceived;

	}

	private int[] getCoordinatorPorts(Properties properties) {

		int[] ports = new int[numberOfDatacenters];

		ports[0] = portMap.get("Port1");
		ports[1] = portMap.get("Port2");
		ports[2] = portMap.get("Port3");

		return ports;
	}

	private String[] getCoordinatorIPAddresses(Properties properties) {

		String[] ipAddresses = new String[numberOfDatacenters];
		ipAddresses[0] = ipMap.get("IP1");
		ipAddresses[1] = ipMap.get("IP2");
		ipAddresses[2] = ipMap.get("IP3");

		return ipAddresses;
	}

	private void setPropertiesFromConfigFile(String configFilePath) {

		Properties properties = new Properties();

		try {
			FileInputStream file = new FileInputStream(configFilePath);
			properties = new Properties();
			properties.load(file);
			file.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		portMap.put("Port1",
				Integer.parseInt(properties.getProperty("DC0-Shard1Port")));
		portMap.put("Port2",
				Integer.parseInt(properties.getProperty("DC1-Shard1Port")));
		portMap.put("Port3",
				Integer.parseInt(properties.getProperty("DC2-Shard1Port")));
		ipMap.put("IP1", properties.getProperty("DC0-Shard1"));
		ipMap.put("IP2", properties.getProperty("DC1-Shard1"));
		ipMap.put("IP3", properties.getProperty("DC2-Shard1"));

	}

}
