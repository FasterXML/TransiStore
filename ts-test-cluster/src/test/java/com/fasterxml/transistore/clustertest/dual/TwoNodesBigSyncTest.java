package com.fasterxml.transistore.clustertest.dual;

import java.io.*;
import java.util.Random;

import org.skife.config.TimeSpan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.StoreOperationSource;

import com.yammer.dropwizard.util.Duration;

import com.fasterxml.clustermate.client.operation.PutOperationResult;
import com.fasterxml.clustermate.dw.RunMode;
import com.fasterxml.clustermate.service.cfg.ClusterConfig;
import com.fasterxml.clustermate.std.ChecksumUtil;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.client.*;
import com.fasterxml.transistore.clustertest.ClusterTestBase;
import com.fasterxml.transistore.clustertest.StoreForTests;
import com.fasterxml.transistore.clustertest.util.FakeHttpRequest;
import com.fasterxml.transistore.clustertest.util.FakeHttpResponse;
import com.fasterxml.transistore.clustertest.util.TimeMasterForClusterTesting;
import com.fasterxml.transistore.dw.BasicTSServiceConfigForDW;

public class TwoNodesBigSyncTest extends ClusterTestBase
{
    final static int PORT_BASE = PORT_BASE_DUAL + PORT_DELTA_SYNC;

    // use ports that differ from other tests, just to minimize chance of
    // collision
    private final static int TEST_PORT1 = PORT_BASE+0;
    private final static int TEST_PORT2 = PORT_BASE+1;

    /**
     * Would be awesome to run with higher number -- test should work for
     * about any number -- but builds get progressively slower. Hence a
     * compromise value seen here.
     */
    private final static int ENTRIES = 1700;

    private final Logger LOG = LoggerFactory.getLogger(getClass());
    
    /* Let's test a sizable dataset, uploading content into just one node,
     * and then let synchronization happen. And once done (by counts),
     * verify that contents are equal.
     */
    public void testBigSync() throws Exception
    {
        initTestLogging(); // reduce noise

        IpAndPort endpoint1 = new IpAndPort("localhost:"+TEST_PORT1);
        IpAndPort endpoint2 = new IpAndPort("localhost:"+TEST_PORT2);

        // use keyspace length of 360 to force specific distribution of entries
        ClusterConfig clusterConfig = twoNodeClusterConfig(endpoint1, endpoint2, 360);
        
        // reduce shutdown grace period to speed up shutdown...
        BasicTSServiceConfigForDW serviceConfig1 = createTwoNodeConfig("fullStack2sync_1",
                TEST_PORT1, clusterConfig);
        
        // all nodes need same (or at least similar enough) cluster config:
        final long START_TIME = 200L;
        final TimeMasterForClusterTesting timeMaster = new TimeMasterForClusterTesting(START_TIME);
        // important: last argument 'true' so that background sync thread gets started
        StoreForTests service1 = StoreForTests.createTestService(serviceConfig1, timeMaster, RunMode.TEST_FULL);
        BasicTSServiceConfigForDW serviceConfig2 = createTwoNodeConfig("fullStack2sync_2",
                TEST_PORT2, clusterConfig);
        StoreForTests service2 = StoreForTests.createTestService(serviceConfig2, timeMaster, RunMode.TEST_FULL);

        // but only start first one...
        startServices(service1);

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
            timeMaster.advanceCurrentTimeMillis(1L);
            Random rnd = new Random(1);
        
            // and start sending stuff!
            long totalBytes = 0L;
            long nextCheck = System.currentTimeMillis() + 1000L; // log progress...
            for (int i = 0; i < ENTRIES; ++i) {
                final BasicTSKey key = generateKey(rnd, i);
                byte[] data = generateData(rnd);
                totalBytes += data.length;
                
                // do occasional PUTs via real client; but mostly direct, latter
                // to speed up test.
                if ((i % 31) == 0) {
                    PutOperationResult put = client.putContent(clientConfig, null, key, data);
                    assertEquals(put.getSuccessCount(), 1);
                } else {
                    FakeHttpResponse response = new FakeHttpResponse();
                    final int inputHash = ChecksumUtil.calcChecksum(data);
                    service1.getStoreHandler().putEntry(new FakeHttpRequest(), response,
                            key, inputHash, new ByteArrayInputStream(data),
                            null, null, null);
                    assertEquals(200, response.getStatus());
                }

                // advance a bit every now and then
                if ((i % 3) == 1) {
                    timeMaster.advanceCurrentTimeMillis(1L);
                }
                if (System.currentTimeMillis() > nextCheck) {
                    nextCheck += 1000L;
                    LOG.warn("Test has sent {}/{} requests", (i+1), ENTRIES);
                }
            }
            LOG.warn("Test sent all {} requests, {} kB of data", ENTRIES, totalBytes);
            // and should now have all the entries in the first store
            assertEquals(ENTRIES, service1.getEntryStore().getEntryCount());

            // Then the second phase; start up second store, let things... sync
            startServices(service2);
            Thread.sleep(10L);
            timeMaster.advanceTimeToWakeAll();
            timeMaster.advanceCurrentTimeMillis(5000L);
            Thread.sleep(10L);
            timeMaster.advanceCurrentTimeMillis(5000L);
            Thread.sleep(10L);
            timeMaster.advanceCurrentTimeMillis(5000L);
            Thread.sleep(10L);

            // and loop for a bit, so that syncing occurs; looks like our rate is rather low..
            final int ROUNDS = 5 + (ENTRIES / 5);
            int i = 0;
            final long start = System.currentTimeMillis();
            
            for (; true; ++i) {
                long entries = service2.getEntryStore().getEntryCount();
                if (entries == ENTRIES) {
                    break;
                }
                if (i > ROUNDS) {
                    fail("Failed to sync after "+ROUNDS+" rounds; entries: "+entries+" (of "+ENTRIES+")");
                }
                timeMaster.advanceCurrentTimeMillis(15000L);
                Thread.sleep(10L);
            }

            LOG.warn("Synced {} entries in {} rounds (of max {}), {} msecs", ENTRIES, i, ROUNDS,
                    System.currentTimeMillis()-start);
            
            // and then, verify that it is all there, in correct shape
            rnd = new Random(1);
            for (i = 0; i < ENTRIES; ++i) {
                verifyEntry(i, rnd, service1, service2);
            }
        } finally {
            // and That's All, Folks!
            service1.prepareForStop();
            service2.prepareForStop();
            Thread.sleep(10L);
            service1._stop();
            service2._stop();
        }
        try { Thread.sleep(20L); } catch (InterruptedException e) { }
        service1.waitForStopped();
        service2.waitForStopped();
    }

    protected BasicTSServiceConfigForDW createTwoNodeConfig(String name, int port, ClusterConfig cluster)
        throws IOException
    {
        final Duration shutdownDelay = Duration.milliseconds(100L);
        return createNodeConfig(name, true, port, cluster)
                .setShutdownGracePeriod(shutdownDelay)
                .setSyncGracePeriod(new TimeSpan("1s"))
                .disableRequestLog();
    }
    
    protected void verifyEntry(int index, Random rnd,
            StoreForTests store1, StoreForTests store2) throws Exception
    {
        final BasicTSKey key = generateKey(rnd, index);
        if (store1.getEntryStore().findEntry(StoreOperationSource.ADMIN_TOOL,
                null, key.asStorableKey()) == null) {
            fail("Entry #"+index+"/"+ENTRIES+" missing from original store");
        }
        Storable copy = store2.getEntryStore().findEntry(StoreOperationSource.ADMIN_TOOL,
                null, key.asStorableKey());
        if (copy == null) {
            fail("Entry #"+index+"/"+ENTRIES+" missing from destination store");
        }
        // plus verify contents...
        byte[] data = generateData(rnd);
        if (data.length != copy.getActualUncompressedLength()) {
            fail("Uncompressed entry #"+index+"/"+ENTRIES+" has wrong length ("+copy.getActualUncompressedLength()+"), expect "+data.length);
        }
    }

    protected BasicTSKey generateKey(Random rnd, int ix) {
        return contentKey("key"+(rnd.nextInt() & 0xF)+"_"+ix);
    }
    
    protected byte[] generateData(Random rnd) {
        return generateData(rnd.nextInt());
    }

    protected byte[] generateData(int rndValue)
    {
        int nibble = (rndValue & 0xF);
        switch (nibble) {
        case 0:
            return new byte[0];
        case 1:
            return new byte[] { (byte) rndValue };
        }
        byte[] result = new byte[3 * (1 << nibble)];
        for (int i = 0, len = result.length; i < len; ++i) {
            result[i] = (byte) rndValue;
            ++rndValue;
        }
        return result;
    }

}
