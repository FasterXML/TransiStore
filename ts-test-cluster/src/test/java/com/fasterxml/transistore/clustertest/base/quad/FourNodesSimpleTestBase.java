package com.fasterxml.transistore.clustertest.base.quad;

import static org.junit.Assert.assertArrayEquals;

import java.util.Arrays;

import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.store.AdminStorableStore;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.StoreOperationSource;

import com.fasterxml.clustermate.client.NodesForKey;
import com.fasterxml.clustermate.client.operation.DeleteOperationResult;
import com.fasterxml.clustermate.client.operation.PutOperationResult;
import com.fasterxml.clustermate.dw.RunMode;
import com.fasterxml.clustermate.service.cfg.ClusterConfig;
import com.fasterxml.clustermate.service.cfg.KeyRangeAllocationStrategy;
import com.fasterxml.clustermate.service.cfg.NodeConfig;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.client.*;
import com.fasterxml.transistore.clustertest.ClusterTestBase;
import com.fasterxml.transistore.clustertest.StoreForTests;
import com.fasterxml.transistore.clustertest.util.TimeMasterForClusterTesting;
import com.fasterxml.transistore.dw.BasicTSServiceConfigForDW;

import com.yammer.dropwizard.util.Duration;

/**
 * Simple CRUD tests for four-node setup (with 100% overlapping key range),
 * using basic two-way replication by client. All nodes run on same JVM.
 * For these tests, we will only send a single copy, to verify the way
 * node-to-node replication works.
 */
public abstract class FourNodesSimpleTestBase extends ClusterTestBase
{
    final static int PORT_BASE = PORT_BASE_QUAD + PORT_DELTA_SIMPLE;
    
    // use ports that differ from other tests, just to minimize chance of
    // collision
    private final static int TEST_PORT1 = PORT_BASE + 0;
    private final static int TEST_PORT2 = PORT_BASE + 1;
    private final static int TEST_PORT3 = PORT_BASE + 2;
    private final static int TEST_PORT4 = PORT_BASE + 3;
    
    public void testSimpleFourNode() throws Exception
    {
//if (true) { System.err.println("Skip 4-node test!"); return; }

        initTestLogging(); // reduce noise

        IpAndPort endpoint1 = new IpAndPort("localhost:"+TEST_PORT1);
        IpAndPort endpoint2 = new IpAndPort("localhost:"+TEST_PORT2);
        IpAndPort endpoint3 = new IpAndPort("localhost:"+TEST_PORT3);
        IpAndPort endpoint4 = new IpAndPort("localhost:"+TEST_PORT4);

        // all nodes need same (or at least similar enough) cluster config:
        final TimeMasterForClusterTesting timeMaster = new TimeMasterForClusterTesting(400L);
        
        // reduce shutdown grace period to speed up shutdown...
        final Duration shutdownDelay = Duration.milliseconds(100L);
        ClusterConfig cc1 =  clusterConfig(endpoint1, endpoint2, endpoint3, endpoint4);
        
        BasicTSServiceConfigForDW serviceConfig1 = createNodeConfig("fullStack4_1", true, TEST_PORT1, cc1)
                .setShutdownGracePeriod(shutdownDelay);
        StoreForTests service1 = StoreForTests.createTestService(serviceConfig1, timeMaster, RunMode.TEST_FULL);        
        BasicTSServiceConfigForDW serviceConfig2 = createNodeConfig("fullStack4_2", true, TEST_PORT2, cc1)
                .setShutdownGracePeriod(shutdownDelay);
        StoreForTests service2 = StoreForTests.createTestService(serviceConfig2, timeMaster, RunMode.TEST_FULL);
        BasicTSServiceConfigForDW serviceConfig3 = createNodeConfig("fullStack4_3", true, TEST_PORT3, cc1)
                .setShutdownGracePeriod(shutdownDelay);
        StoreForTests service3 = StoreForTests.createTestService(serviceConfig3, timeMaster, RunMode.TEST_FULL);
        BasicTSServiceConfigForDW serviceConfig4 = createNodeConfig("fullStack4_4", true, TEST_PORT4, cc1)
                .setShutdownGracePeriod(shutdownDelay);
        StoreForTests service4 = StoreForTests.createTestService(serviceConfig4, timeMaster, RunMode.TEST_FULL);

        // need to do some acrobatics to only try clean shutdown when things work...
        boolean passed = false; 
//        int rounds = 0;
        
        // then start them all
        startServices(service1, service2, service3, service4);

        try {
            // require just a single ok, so we can test replication nicely
            BasicTSClientConfig clientConfig = new BasicTSClientConfigBuilder()
	            .setMinimalOksToSucceed(1)
	            .setOptimalOks(1)
	            .setMaxOks(1)
	            .setAllowRetries(false) // let's not give tests too much slack, shouldn't need it
	            .build();
            BasicTSClient client = createClient(clientConfig, endpoint1, endpoint2, endpoint3, endpoint4);
            
            /* at this point we can let server complete its initial startup,
             * which may include cleanup (test mode usually adds something like
             * 1 msec virtual sleep)
             */
            service1.getTimeMaster().advanceCurrentTimeMillis(1L);
            
            final BasicTSKey KEY = contentKey("testSimple4-item1");

            // first things first: while hash is arbitrary, it should be constant, so:
            int routingHash = service1.getKeyConverter().routingHashFor(KEY);
            assertEquals(530147840, routingHash);
            // and we happen to know that primary is 'service4', secondary 'service3'
            NodesForKey nodes = client.getCluster().getNodesFor(KEY);
            assertEquals(2, nodes.size());
            assertEquals("[270,+180]", nodes.node(0).getActiveRange().toString());
            assertEquals("[180,+180]", nodes.node(1).getActiveRange().toString());
            
            // first: verify that we can do GET, but not find the entry:
            byte[] data = client.getContentAsBytes(clientConfig, KEY);
            assertNull("Should not yet have entry", data);

            // Then add said content
            final byte[] CONTENT = new byte[12000];
            Arrays.fill(CONTENT, (byte) 0xAC);
            PutOperationResult result = client.putContent(clientConfig, null, KEY, CONTENT);
            assertTrue(result.succeededOptimally());
            assertEquals(result.getSuccessCount(), 1);
            // and advance time (to get distinct timestamps)
            service1.getTimeMaster().advanceCurrentTimeMillis(1L);
    
            // find it; both with GET and HEAD
            data = client.getContentAsBytes(clientConfig, KEY);
            assertNotNull("Should now have the data", data);
            assertArrayEquals(CONTENT, data);

            long len = client.getContentLength(clientConfig, KEY);
            assertEquals(12000L, len);
            service1.getTimeMaster().advanceCurrentTimeMillis(1L);
    
            // At this point, verify it is held by... who should it be?import com.fasterxml.storemate.api.ClientId;

            int keyHash = service1.getKeyConverter().routingHashFor(KEY);
            assertEquals(530147840, keyHash); // calculates separately
            
            // turns out it should be last one (0x3 with modulo of 0x3)
            Storable entry;
            entry = service1.getEntryStore().findEntry(StoreOperationSource.REQUEST, null, KEY.asStorableKey());
            assertNull(entry);
            assertEquals(0, entryCount(service1.getEntryStore()));
            entry = service2.getEntryStore().findEntry(StoreOperationSource.REQUEST, null, KEY.asStorableKey());
            assertNull(entry);
            assertEquals(0, entryCount(service2.getEntryStore()));
            entry = service3.getEntryStore().findEntry(StoreOperationSource.REQUEST, null, KEY.asStorableKey());
            assertNull(entry);
            assertEquals(0, entryCount(service3.getEntryStore()));
            entry = service4.getEntryStore().findEntry(StoreOperationSource.REQUEST, null, KEY.asStorableKey());
            assertNotNull(entry);
            assertEquals(1, entryCount(service4.getEntryStore()));

            // Now: let's let nodes synchronize their state. This may require bit of waiting;
            long need = service1.getTimeMaster().getMaxSleepTimeNeeded();
            
            // should need to sleep a bit; assume it's at least 10 msec, at most 10 secs
            if (need < 10L || need > 10000L) {
                fail("Expected having to sleep between 10ms and 10s; got: "+need+" ms");
            }
            service1.getTimeMaster().advanceTimeToWakeAll();

            /* Looks like multiple sleep/resume cycles are needed to get things
             * to converge to steady state. Try to advance, sleep, a few times.
             */
            // FWIW, we seem to require about 7-8 rounds (!) to get it all done
            /*rounds =*/ expectState("0/0/1/1", "Entry should be copied into second node", 12, 18,
                    service1, service2, service3, service4);
// System.err.println("Took "+rounds+" rounds to PUT");            
            
            // Then finally delete; will only initially delete from #4
            DeleteOperationResult del = client.deleteContent(clientConfig, KEY);
            assertTrue(del.succeededMinimally());
            assertTrue(del.succeededOptimally());
            assertEquals(del.getSuccessCount(), 1);

            // Ok: now, keeping in mind that deletions are done by using tombstone as marker...
            assertEquals("Store #4 should now have tombstone", "0/0/1/1(1)",
                    storeCounts(service1, service2, service3, service4));

            /* At this point things are bit murky: should client try to find a copy
             * (despite a tombstone) or not? Current implementation will consider
             * tombstone the definite answer; so that is the "right answer" here,
             * although it does require that client does calls in strict priority order.
             */
            data = client.getContentAsBytes(clientConfig, KEY);
            if (data != null && data.length > 0) {
                fail("Should not have the data after partial DELETE: got entry with "+data.length+" bytes");
            }

            // but then tombstone should propagate; faster than PUT propagation
            /*rounds =*/ expectState("0/0/1(1)/1(1)", "Entry should have tombstone for both nodes", 5, 10,
                    service1, service2, service3, service4);         

// System.err.println("Took "+rounds+" rounds to DELETE");            
            
            // after which content ... is no more:
            data = client.getContentAsBytes(clientConfig, KEY);
            if (data != null && data.length > 0) {
                fail("Should not have the data after full DELETE: got entry with "+data.length+" bytes");
            }

            assertEquals("Store #4 should have 1 tombstone", 1,
                    ((AdminStorableStore) service4.getEntryStore()).getTombstoneCount(StoreOperationSource.ADMIN_TOOL, 3000L));
            assertEquals("Store #3 should have 1 tombstone", 1,
                    ((AdminStorableStore) service3.getEntryStore()).getTombstoneCount(StoreOperationSource.ADMIN_TOOL, 3000L));
            
            // and we may clean tombstones, too
            assertEquals("Should have removed 1 tombstone from store #4", 1,
                    ((AdminStorableStore) service4.getEntryStore()).removeTombstones(StoreOperationSource.ADMIN_TOOL, 100));
            assertEquals("Should have removed 1 tombstone from store #3", 1,
                    ((AdminStorableStore) service3.getEntryStore()).removeTombstones(StoreOperationSource.ADMIN_TOOL, 100));
            assertEquals("Shouldn't have tombstones, entries, any more", "0/0/0/0",
                    storeCounts(service1, service2, service3, service4));
            
            // and That's All, Folks!
            passed = true;            

        } finally {
            try {
                // important: first ask kindly for threads to stop, for all servers
                service1.prepareForStop();
                service2.prepareForStop();
                service3.prepareForStop();
                service4.prepareForStop();
                
                Thread.sleep(10L);
 
                // and only then force stopping of the Servlet container (Jetty):
                service1._stop();
                service2._stop();
                service3._stop();
                service4._stop();

                if (passed) { // takes a bit for threads to stop it seems...
                    Thread.sleep(10L);
                }
            } catch (Exception e) {
                System.err.println("Test shutdown failed: "+e.getMessage());
            }
        }
    }

    private ClusterConfig clusterConfig(IpAndPort endpoint1,
            IpAndPort endpoint2, IpAndPort endpoint3, IpAndPort endpoint4)
    {
        ClusterConfig clusterConfig = new ClusterConfig();
        // let's use simple linear, as that's what we want
        clusterConfig.type = KeyRangeAllocationStrategy.SIMPLE_LINEAR;
        clusterConfig.numberOfCopies = 2;
        clusterConfig.clusterKeyspaceSize = 360; // the usual 360 degrees, but divided in 4 overlapping slices
        clusterConfig.clusterNodes.add(new NodeConfig(endpoint1));
        clusterConfig.clusterNodes.add(new NodeConfig(endpoint2));
        clusterConfig.clusterNodes.add(new NodeConfig(endpoint3));
        clusterConfig.clusterNodes.add(new NodeConfig(endpoint4));
        return clusterConfig;
    }
}
