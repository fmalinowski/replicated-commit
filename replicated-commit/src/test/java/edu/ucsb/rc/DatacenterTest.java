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

import edu.ucsb.rc.protocols.PaxosAcceptsManager;
import edu.ucsb.rc.protocols.TwoPhaseCommitManager;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Shard.class})
public class DatacenterTest {

	@After
	public void tearDown() throws Exception {
		MultiDatacenter.getInstance().removeAllDatacenters();
	}

	@Test
	public void testInitializeShards() throws Exception {
		Shard shard0a = new Shard();
		Shard shard0b = new Shard();
		Shard shard0c = new Shard();
		Datacenter dc0 = new Datacenter();
		dc0.setDatacenterID(0);
		
		shard0a.setShardID(0);
		shard0b.setShardID(1);
		shard0c.setShardID(2);
		
		dc0.addShard(shard0a);
		dc0.addShard(shard0b);
		dc0.addShard(shard0c);
		
		MultiDatacenter.getInstance().addDatacenter(dc0);
		
		/* 
		 * Expect the constructor of TwoPhaseCommitManager to be called 3 times
		 * Expect the constructor of PaxosAcceptsManager to be called 3 times 
		 */
		TwoPhaseCommitManager twoPCmMock = PowerMock.createMock(TwoPhaseCommitManager.class);
		PaxosAcceptsManager pamMock = PowerMock.createMock(PaxosAcceptsManager.class);
		try {			
			PowerMock.expectNew(TwoPhaseCommitManager.class, 3).andReturn(twoPCmMock).times(3);
			PowerMock.expectNew(PaxosAcceptsManager.class, 1).andReturn(pamMock).times(3);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		/* We're done with the configuration of the mocks and expectations. Let's test now */
		PowerMock.replayAll();
		
		dc0.initializeShards();
		
		/* Make sure all the expected calls were made */
		PowerMock.verifyAll();
	}

	@Test
	public void testGetShards() throws Exception {
		Datacenter dc = new Datacenter();
				
		Shard shard0 = new Shard();
		Shard shard1 = new Shard();
		Shard shard2 = new Shard();
		
		shard0.setShardID(0);
		shard1.setShardID(1);
		shard2.setShardID(2);
		
		dc.addShard(shard0);
		dc.addShard(shard1);
		dc.addShard(shard2);
		
		ArrayList<Shard> allShards = dc.getShards();
		assertEquals(3, allShards.size());
		
		boolean[] presence = new boolean[3];
		for (Shard shard : allShards) {
			presence[shard.getShardID()] = true;
		}
		for (int i = 0; i < 3; i++) {
			assertTrue(presence[i]);
		}
	}

	@Test
	public void testAddAndGetShard() throws Exception {
		Datacenter dc = new Datacenter();
		
		Shard shard0 = new Shard();
		Shard shard1 = new Shard();
		Shard shard2 = new Shard();
		
		shard1.setShardID(1);
		shard0.setShardID(0);
		shard2.setShardID(2);
		
		dc.addShard(shard2);
		dc.addShard(shard0);
		dc.addShard(shard1);
		
		assertEquals(dc, shard0.getDatacenter());
		assertEquals(dc, shard1.getDatacenter());
		assertEquals(dc, shard2.getDatacenter());
		
		assertEquals(shard0, dc.getShard(0));
		assertEquals(shard1, dc.getShard(1));
		assertEquals(shard2, dc.getShard(2));
	}

	@Test
	public void testGetShardIdForKey() {
		Datacenter dc = new Datacenter();
		
		assertEquals(1, dc.getShardIdForKey("a"));
		assertEquals(2, dc.getShardIdForKey("b"));
		assertEquals(0, dc.getShardIdForKey("c"));
		assertEquals(1, dc.getShardIdForKey("d"));
	}

}
