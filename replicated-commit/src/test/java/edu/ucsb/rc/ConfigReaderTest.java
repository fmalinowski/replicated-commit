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
		
		Datacenter dc0 = multiDatacenter.getDatacenter(0);
		Datacenter dc1 = multiDatacenter.getDatacenter(1);
		Datacenter dc2 = multiDatacenter.getDatacenter(2);
		
		Shard shard0a = dc0.getShard(0);
		Shard shard0b = dc0.getShard(1);
		Shard shard0c = dc0.getShard(2);
		Shard shard1a = dc1.getShard(0);
		Shard shard1b = dc1.getShard(1);
		Shard shard1c = dc1.getShard(2);
		Shard shard2a = dc2.getShard(0);
		Shard shard2b = dc2.getShard(1);
		Shard shard2c = dc2.getShard(2);
		
		assertEquals(dc0, shard0a.getDatacenter());
		assertEquals(dc0, shard0b.getDatacenter());
		assertEquals(dc0, shard0c.getDatacenter());
		assertEquals(dc1, shard1a.getDatacenter());
		assertEquals(dc1, shard1b.getDatacenter());
		assertEquals(dc1, shard1c.getDatacenter());
		assertEquals(dc2, shard2a.getDatacenter());
		assertEquals(dc2, shard2b.getDatacenter());
		assertEquals(dc2, shard2c.getDatacenter());
	}

}
