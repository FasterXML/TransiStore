package com.fasterxml.transistore.clustertest.dual;

import static org.junit.Assert.assertArrayEquals;

import java.io.*;
import java.util.Arrays;

import org.junit.Assert;

import com.yammer.dropwizard.util.Duration;

import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.compress.Compressors;
import com.fasterxml.storemate.store.Storable;

import com.fasterxml.clustermate.client.NodesForKey;
import com.fasterxml.clustermate.client.operation.DeleteOperationResult;
import com.fasterxml.clustermate.client.operation.PutOperationResult;
import com.fasterxml.clustermate.service.cfg.ClusterConfig;
import com.fasterxml.clustermate.service.cluster.ClusterPeer;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.client.*;
import com.fasterxml.transistore.clustertest.ClusterTestBase;
import com.fasterxml.transistore.clustertest.StoreForTests;
import com.fasterxml.transistore.clustertest.util.TimeMasterForClusterTesting;
import com.fasterxml.transistore.dw.BasicTSServiceConfigForDW;

/**
 * Simple CRUD tests for two-node setup (with 100% overlapping key range),
 * using basic two-way replication by client. Both nodes run on same JVM.
 */
public class TwoNodesSimpleTest extends ClusterTestBase
{
    // use ports that differ from other tests, just to minimize chance of
    // collision
    private final static int TEST_PORT1 = 9020;
    private final static int TEST_PORT2 = 9021;
    
    public void testSimpleTwoNode() throws Exception
    {
        initTestLogging(); // reduce noise

        IpAndPort endpoint1 = new IpAndPort("localhost:"+TEST_PORT1);
        IpAndPort endpoint2 = new IpAndPort("localhost:"+TEST_PORT2);

        // both nodes need same (or at least similar enough) cluster config:
        ClusterConfig clusterConfig = twoNodeClusterConfig(endpoint1, endpoint2, 100);

        BasicTSServiceConfigForDW serviceConfig1 = createNodeConfig("fullStack2_1", true, TEST_PORT1, clusterConfig);
        final TimeMasterForClusterTesting timeMaster = new TimeMasterForClusterTesting(200L);
        
        // false -> don't bother with full init of background tasks:
        StoreForTests service1 = StoreForTests.createTestService(serviceConfig1, timeMaster, false);
        
        BasicTSServiceConfigForDW serviceConfig2 = createNodeConfig("fullStack2_2", true, TEST_PORT2, clusterConfig);
        serviceConfig2.getServiceConfig().cluster = clusterConfig;
        StoreForTests service2 = StoreForTests.createTestService(serviceConfig2, timeMaster, false);

        // then start both
        startServices(service1, service2);

        // require 2 OKs, to ensure both get the same data...
        BasicTSClientConfig clientConfig = new BasicTSClientConfigBuilder()
                .setMinimalOksToSucceed(2)
                .setOptimalOks(2)
                .setMaxOks(2)
                .setAllowRetries(false) // let's not give tests too much slack, shouldn't need it
                .build();
        BasicTSClient client = createClient(clientConfig, endpoint1, endpoint2);

        // just for fun, use a space, slash and ampersand in key (to ensure correct encoding)
        final BasicTSKey KEY = contentKey("testSimple2/this&that/some item");
        
        // first: verify that we can do GET, but not find the entry:
        byte[] data = client.getContentAsBytes(clientConfig, KEY);
        assertNull("Should not yet have entry", data);

        // Then add said content
        final byte[] CONTENT = new byte[12000];
        Arrays.fill(CONTENT, (byte) 0xAC);
        PutOperationResult result = client.putContent(clientConfig, KEY, CONTENT);
        assertTrue(result.succeededOptimally());
        assertEquals(result.getSuccessCount(), 2);

        // find it; both with GET and HEAD
        data = client.getContentAsBytes(clientConfig, KEY);
        assertNotNull("Should now have the data", data);
        assertArrayEquals(CONTENT, data);
        long len = client.getContentLength(clientConfig, KEY);
        /* NOTE: should be getting uncompressed length, assuming we don't
         * claim we accept things as compresed (if we did, we'd get 48)
         */
        assertEquals(12000L, len);

        // delete:
        DeleteOperationResult del = client.deleteContent(clientConfig, KEY);
        assertTrue(del.succeededMinimally());
        assertTrue(del.succeededOptimally());
        assertEquals(del.getSuccessCount(), 2);

        // after which content ... is no more:
        data = client.getContentAsBytes(clientConfig, KEY);
        assertNotNull("Should not have the data after DELETE", data);

        // and That's All, Folks!
        
        service1._stop();
        service2._stop();
        service1.waitForStopped();
        service2.waitForStopped();
    }

    /**
     * Unit test that sets up 2-node fully replicated cluster, updates only one host
     * with two entries, and ensures that sync happens correctly.
     */
    public void testTwoNodeSync() throws Exception
    {
        initTestLogging(); // reduce noise

        IpAndPort endpoint1 = new IpAndPort("localhost:"+TEST_PORT1);
        IpAndPort endpoint2 = new IpAndPort("localhost:"+TEST_PORT2);

        // use keyspace length of 360 to force specific distribution of entries
        ClusterConfig clusterConfig = twoNodeClusterConfig(endpoint1, endpoint2, 360);
        
        // reduce shutdown grace period to speed up shutdown...
        final Duration shutdownDelay = Duration.milliseconds(100L);
        BasicTSServiceConfigForDW serviceConfig1 = createNodeConfig("fullStack2b_1", true, TEST_PORT1, clusterConfig)
        		.setShutdownGracePeriod(shutdownDelay);
        // all nodes need same (or at least similar enough) cluster config:
        final long START_TIME = 200L;
        final TimeMasterForClusterTesting timeMaster = new TimeMasterForClusterTesting(START_TIME);
        // important: last argument 'true' so that background sync thread gets started
        StoreForTests service1 = StoreForTests.createTestService(serviceConfig1, timeMaster, true);
        BasicTSServiceConfigForDW serviceConfig2 = createNodeConfig("fullStack2b_2", true, TEST_PORT2, clusterConfig)
        		.setShutdownGracePeriod(shutdownDelay);
        serviceConfig2.getServiceConfig().cluster = clusterConfig;
        StoreForTests service2 = StoreForTests.createTestService(serviceConfig2, timeMaster, true);

        startServices(service1, service2);
        try {
            // only 1 ok so that we can verify syncing
            BasicTSClientConfig clientConfig = new BasicTSClientConfigBuilder()
                .setMinimalOksToSucceed(1)
                .setOptimalOks(1)
                .setMaxOks(1)
                .setAllowRetries(false) // let's not give tests too much slack, shouldn't need it
                .build();
            BasicTSClient client = createClient(clientConfig, endpoint1, endpoint2);
            /* at this point we can let server complete its initial startup,
             * which may include cleanup (test mode usually adds something like
             * 1 msec virtual sleep)
             */
            timeMaster.advanceCurrentTimeMillis(1L); // to 201
	
            // just for fun, use a space, slash and ampersand in key (to ensure correct encoding)
            final BasicTSKey KEY1 = contentKey("testSimple2a");
            final BasicTSKey KEY2 = contentKey("testSimple2b");
	
            // both keys should map to server2, so that we get imbalance
            final NodesForKey nodes1 = client.getCluster().getNodesFor(KEY1);
            assertEquals(2, nodes1.size());

            // we used keyspace length of 360, so:
            assertEquals("[180,+360]", nodes1.node(0).getActiveRange().toString());
            assertEquals(1530323550, service1.getKeyConverter().routingHashFor(KEY1));
	
            final NodesForKey nodes2 = client.getCluster().getNodesFor(KEY2);
            assertEquals(2, nodes2.size());
            assertEquals("[180,+360]", nodes2.node(0).getActiveRange().toString());
            assertEquals(937517304, service1.getKeyConverter().routingHashFor(KEY2));
            
            assertNotSame(service1.getEntryStore(), service2.getEntryStore());
            assertEquals(0, service1.getEntryStore().getEntryCount());
            assertEquals(0, service2.getEntryStore().getEntryCount());

            // Ok, so, add 2 entries on store 2:
	        
            final byte[] CONTENT1 = biggerSomewhatCompressibleData(128000);
            final byte[] CONTENT1_LZF = lzfCompress(CONTENT1);
            final byte[] CONTENT2 = biggerSomewhatCompressibleData(19000);
            final byte[] CONTENT2_LZF = lzfCompress(CONTENT2);
            PutOperationResult put = client.putContent(clientConfig, KEY1, CONTENT1);
            assertTrue(put.succeededOptimally());
            assertEquals(put.getSuccessCount(), 1);
            timeMaster.advanceCurrentTimeMillis(1L); // to 202
            put = client.putContent(clientConfig, KEY2, CONTENT2);
            assertTrue(put.succeededOptimally());
            assertEquals(put.getSuccessCount(), 1);
            
            assertEquals(0, service1.getEntryStore().getEntryCount());
            assertEquals(2, service2.getEntryStore().getEntryCount());

            // sanity check: verify that entry/entries were uploaded with full info
            Storable entry1 = service2.getEntryStore().findEntry(KEY1.asStorableKey());
            assertNotNull(entry1);
            assertEquals(CONTENT1.length, entry1.getOriginalLength());
            assertEquals(Compression.LZF, entry1.getCompression());
            assertEquals(CONTENT1_LZF.length, entry1.getStorageLength());
            File f = entry1.getExternalFile(service2.getFileManager());

            verifyHash("content hash for entry #1", entry1.getContentHash(), CONTENT1);
            byte[] b = readAll(f);
            verifyHash("compressed hash for entry #1", entry1.getCompressedHash(), readAll(f));
            byte[] b2 = Compressors.lzfUncompress(b);
            Assert.assertArrayEquals(CONTENT1, b2);

            assertEquals(START_TIME + 1L, entry1.getLastModified());

            Storable entry2 = service2.getEntryStore().findEntry(KEY2.asStorableKey());
            assertNotNull(entry2);
            assertEquals(CONTENT2.length, entry2.getOriginalLength());
            assertEquals(Compression.LZF, entry2.getCompression());
            assertEquals(CONTENT2_LZF.length, entry2.getStorageLength());

            f = entry2.getExternalFile(service2.getFileManager());
            verifyHash("content hash for entry #2", entry2.getContentHash(), CONTENT2);
            b = readAll(f);
            verifyHash("compressed hash for entry #1", entry2.getCompressedHash(), b);
            b2 = Compressors.lzfUncompress(b);
            Assert.assertArrayEquals(CONTENT2, b2);

            assertEquals(START_TIME + 2L, entry2.getLastModified());
            
            // verify they are visible from #2, not #1, 
            assertEquals(2, service2.getEntryStore().getEntryCount());
            assertEquals("Should not have yet propagated entries to store #1",
	        		0, service1.getEntryStore().getEntryCount());

            // ok. start syncing:
            timeMaster.advanceTimeToWakeAll();

            /* Looks like multiple sleep/resume cycles are needed to get things
             * to converge to steady state. Try to advance, sleep, a few times.
             */
            // FWIW, we seem to require about 7-8 rounds (!) to get it all done
            /*int rounds =*/ expectState("2/2", "Entry should be copied into first node", 5, 10,
                    service1, service2);
	
            // but wait! Let's verify that stuff is moved without corruption...
            Storable entryCopy1 = service1.getEntryStore().findEntry(KEY1.asStorableKey());
            assertNotNull(entryCopy1);
            assertEquals(entry1.getOriginalLength(), entryCopy1.getOriginalLength());
            assertEquals(entry1.getStorageLength(), entryCopy1.getStorageLength());
            assertTrue(entryCopy1.hasExternalData());
            f = entryCopy1.getExternalFile(service1.getFileManager());
            assertTrue(f.exists());
            assertEquals(entry1.getStorageLength(), f.length());
            assertEquals(entry1.getContentHash(), entryCopy1.getContentHash());
            assertEquals(entry1.getCompressedHash(), entryCopy1.getCompressedHash());

            Storable entryCopy2 = service1.getEntryStore().findEntry(KEY2.asStorableKey());
            assertNotNull(entryCopy2);
            assertEquals(entry2.getOriginalLength(), entryCopy2.getOriginalLength());
            assertEquals(entry2.getStorageLength(), entryCopy2.getStorageLength());
            assertEquals(entry2.getContentHash(), entryCopy2.getContentHash());
            assertEquals(entry2.getCompressedHash(), entryCopy2.getCompressedHash());

            // and finally, ensure that background sync threads did not encounter issues
            // ... but looks that might take a while, too
            for (int i = 0; ; ++i) {
                boolean fail = (i == 3);
                if (_verifyClusterPeer("service 1", service1, START_TIME+3L, fail)
                    && _verifyClusterPeer("service 2", service1, START_TIME+3L, fail)) {
                    break;
                }
                timeMaster.advanceCurrentTimeMillis(15000L);
                Thread.sleep(20L);
            }
            
        } finally {
            // and That's All, Folks!
            service1.prepareForStop();
            service2.prepareForStop();
            service1._stop();
            service2._stop();
        }
        try { Thread.sleep(20L); } catch (InterruptedException e) { }
        service1.waitForStopped();
        service2.waitForStopped();
    }

    protected boolean _verifyClusterPeer(String desc, StoreForTests store,
            long minSyncedUpTo, boolean fail)
    {
        for (ClusterPeer peer : store.getCluster().getPeers()) {
            if (peer.getFailCount() != 0) {
                if (fail) {
                    fail("Problem with "+desc+", peer for "+peer.getAddress()+" has "+peer.getFailCount()+" fails");
                }
                return false;
            }
            if (peer.getSyncedUpTo() < minSyncedUpTo) {
                if (fail) {
                    fail("Problem with "+desc+", peer for "+peer.getAddress()+" only synced up to "+peer.getSyncedUpTo()+", should have at least "+minSyncedUpTo);
                }
                return false;
            }
        }
        return true;
    }
}
