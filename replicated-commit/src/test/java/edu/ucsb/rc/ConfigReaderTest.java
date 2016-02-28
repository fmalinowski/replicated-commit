package edu.ucsb.rc;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ConfigReaderTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
		MultiDatacenter.getInstance().removeAllDatacenters();
	}
	
	@Test
	public void testGetShardListeningPort() {
		ConfigReader configReader;
		configReader = new ConfigReader("config-unit-test.properties");
		
		assertEquals(50000, configReader.getShardListeningPort());
		assertEquals(50001, configReader.getClientListeningPort());
	}
	
	@Test
	public void testGetClientListeningPort() {
		ConfigReader configReader;
		configReader = new ConfigReader("config-unit-test.properties");
		
		assertEquals(50001, configReader.getClientListeningPort());
	}

	@Test
	public void testInitializeMultiDatacenter() {
		ConfigReader configReader;
		MultiDatacenter multiDatacenter;
		
		configReader = new ConfigReader("config-unit-test.properties");
		
		multiDatacenter = configReader.initializeMultiDatacenter();
		assertEquals(3, multiDatacenter.getDatacenters().size());
		assertEquals(3, multiDatacenter.getDatacenter(0).getShards().size());
		assertEquals(3, multiDatacenter.getDatacenter(1).getShards().size());
		assertEquals(3, multiDatacenter.getDatacenter(2).getShards().size());
		
		assertEquals("81.123.123.251", multiDatacenter.getDatacenter(0).getShard(0).getIpAddress());
		assertEquals("81.123.231.123", multiDatacenter.getDatacenter(0).getShard(1).getIpAddress());
		assertEquals("81.123.211.111", multiDatacenter.getDatacenter(0).getShard(2).getIpAddress());
		assertEquals("81.001.123.251", multiDatacenter.getDatacenter(1).getShard(0).getIpAddress());
		assertEquals("81.001.231.123", multiDatacenter.getDatacenter(1).getShard(1).getIpAddress());
		assertEquals("81.001.211.111", multiDatacenter.getDatacenter(1).getShard(2).getIpAddress());
		assertEquals("81.251.123.251", multiDatacenter.getDatacenter(2).getShard(0).getIpAddress());
		assertEquals("81.251.231.123", multiDatacenter.getDatacenter(2).getShard(1).getIpAddress());
		assertEquals("81.251.211.111", multiDatacenter.getDatacenter(2).getShard(2).getIpAddress());
		
		assertEquals(1, multiDatacenter.getCurrentDatacenter().getDatacenterID());
		assertEquals(2, multiDatacenter.getCurrentShard().getShardID());
	}

}
