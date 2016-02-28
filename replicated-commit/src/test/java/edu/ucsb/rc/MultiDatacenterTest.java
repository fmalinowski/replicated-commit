package edu.ucsb.rc;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import edu.ucsb.rc.network.Message;
import edu.ucsb.rc.network.NetworkHandler;
import edu.ucsb.rc.protocols.PaxosAcceptsManager;
import edu.ucsb.rc.protocols.TwoPhaseCommitManager;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Shard.class})
public class MultiDatacenterTest {
	
	@After
	public void tearDown() throws Exception {
		MultiDatacenter.getInstance().removeAllDatacenters();
	}

	@Test
	public void testGetDatacenters() throws Exception {
		MultiDatacenter multiDatacenter = MultiDatacenter.getInstance();
		
		Datacenter dc1 = new Datacenter();
		Datacenter dc2 = new Datacenter();
		Datacenter dc3 = new Datacenter();
		
		dc1.setDatacenterID(0);
		dc2.setDatacenterID(1);
		dc3.setDatacenterID(2);
		
		multiDatacenter.addDatacenter(dc2);
		multiDatacenter.addDatacenter(dc1);
		multiDatacenter.addDatacenter(dc3);
		
		ArrayList<Datacenter> allDatacenters = multiDatacenter.getDatacenters();
		assertEquals(3, allDatacenters.size());
		
		boolean[] presence = new boolean[3];
		for (Datacenter dc : allDatacenters) {
			presence[dc.getDatacenterID()] = true;
		}
		for (int i = 0; i < 3; i++) {
			assertTrue(presence[i]);
		}
	}

	@Test
	public void testAddAndGetDatacenter() throws Exception {
		MultiDatacenter multiDatacenter = MultiDatacenter.getInstance();
		
		Datacenter dc1 = new Datacenter();
		Datacenter dc2 = new Datacenter();
		Datacenter dc3 = new Datacenter();
		
		dc1.setDatacenterID(1);
		dc2.setDatacenterID(2);
		dc3.setDatacenterID(3);
		
		multiDatacenter.addDatacenter(dc2);
		multiDatacenter.addDatacenter(dc1);
		multiDatacenter.addDatacenter(dc3);
		
		assertEquals(dc1, multiDatacenter.getDatacenter(1));
		assertEquals(dc2, multiDatacenter.getDatacenter(2));
		assertEquals(dc3, multiDatacenter.getDatacenter(3));
	}

	@Test
	public void testInitializeShards() throws Exception {
		Shard shard0a = new Shard();
		Shard shard0b = new Shard();
		Shard shard0c = new Shard();
		Shard shard1a = new Shard();
		Shard shard1b = new Shard();
		Shard shard1c = new Shard();
		Datacenter dc0 = new Datacenter();
		Datacenter dc1 = new Datacenter();
		
		dc0.setDatacenterID(0);
		dc1.setDatacenterID(1);
		shard0a.setShardID(0);
		shard0b.setShardID(1);
		shard0c.setShardID(2);
		shard1a.setShardID(0);
		shard1b.setShardID(1);
		shard1c.setShardID(2);
		
		dc0.addShard(shard0a);
		dc0.addShard(shard0b);
		dc0.addShard(shard0c);
		dc1.addShard(shard1a);
		dc1.addShard(shard1b);
		dc1.addShard(shard1c);
		
		MultiDatacenter multiDatacenter = MultiDatacenter.getInstance();
		multiDatacenter.addDatacenter(dc0);
		multiDatacenter.addDatacenter(dc1);
		
		/* 
		 * Expect the constructor of TwoPhaseCommitManager to be called 6 times
		 * Expect the constructor of PaxosAcceptsManager to be called 6 times 
		 */
		TwoPhaseCommitManager twoPCmMock = PowerMock.createMock(TwoPhaseCommitManager.class);
		PaxosAcceptsManager pamMock = PowerMock.createMock(PaxosAcceptsManager.class);
		try {			
			PowerMock.expectNew(TwoPhaseCommitManager.class, 3).andReturn(twoPCmMock).times(6);
			PowerMock.expectNew(PaxosAcceptsManager.class, 2).andReturn(pamMock).times(6);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		/* We're done with the configuration of the mocks and expectations. Let's test now */
		PowerMock.replayAll();
		
		multiDatacenter.initializeShards();
		
		/* Make sure all the expected calls were made */
		PowerMock.verifyAll();
	}

	@Test
	public void testGetOtherShardsWithId() throws Exception {
		Shard shard0a = new Shard();
		Shard shard0b = new Shard();
		Shard shard0c = new Shard();
		Shard shard1a = new Shard();
		Shard shard1b = new Shard();
		Shard shard1c = new Shard();
		Shard shard2a = new Shard();
		Shard shard2b = new Shard();
		Shard shard2c = new Shard();
		Datacenter dc0 = new Datacenter();
		Datacenter dc1 = new Datacenter();
		Datacenter dc2 = new Datacenter();
		
		dc0.setDatacenterID(0);
		dc1.setDatacenterID(1);
		dc2.setDatacenterID(2);
		shard0a.setShardID(0);
		shard0b.setShardID(1);
		shard0c.setShardID(2);
		shard1a.setShardID(0);
		shard1b.setShardID(1);
		shard1c.setShardID(2);
		shard2a.setShardID(0);
		shard2b.setShardID(1);
		shard2c.setShardID(2);
		
		dc0.addShard(shard0a);
		dc0.addShard(shard0b);
		dc0.addShard(shard0c);
		dc1.addShard(shard1a);
		dc1.addShard(shard1b);
		dc1.addShard(shard1c);
		dc2.addShard(shard2a);
		dc2.addShard(shard2b);
		dc2.addShard(shard2c);
		
		MultiDatacenter multiDatacenter = MultiDatacenter.getInstance();
		multiDatacenter.addDatacenter(dc0);
		multiDatacenter.addDatacenter(dc1);
		multiDatacenter.addDatacenter(dc2);
		multiDatacenter.setCurrentDatacenter(dc1);
		
		ArrayList<Shard> otherShards = multiDatacenter.getOtherShardsWithId(0);
		assertEquals(2, otherShards.size());
		assertTrue(otherShards.contains(shard0a));
		assertTrue(otherShards.contains(shard2a));
		
		otherShards = multiDatacenter.getOtherShardsWithId(1);
		assertEquals(2, otherShards.size());
		assertTrue(otherShards.contains(shard0b));
		assertTrue(otherShards.contains(shard2b));
		
		multiDatacenter.setCurrentDatacenter(dc0);
		
		otherShards = multiDatacenter.getOtherShardsWithId(2);
		assertEquals(2, otherShards.size());
		assertTrue(otherShards.contains(shard1c));
		assertTrue(otherShards.contains(shard2c));
	}

}
