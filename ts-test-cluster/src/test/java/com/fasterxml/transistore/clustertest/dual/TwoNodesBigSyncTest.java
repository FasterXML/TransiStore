package com.fasterxml.transistore.clustertest.dual;

import java.util.Random;

import org.skife.config.TimeSpan;

import com.fasterxml.clustermate.client.operation.PutOperationResult;
import com.fasterxml.clustermate.service.cfg.ClusterConfig;
import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.client.BasicTSClient;
import com.fasterxml.transistore.client.BasicTSClientConfig;
import com.fasterxml.transistore.client.BasicTSClientConfigBuilder;
import com.fasterxml.transistore.client.ahc.AHCBasedClientBootstrapper;
import com.fasterxml.transistore.clustertest.ClusterTestBase;
import com.fasterxml.transistore.clustertest.StoreForTests;
import com.fasterxml.transistore.clustertest.util.TimeMasterForClusterTesting;
import com.fasterxml.transistore.dw.BasicTSServiceConfigForDW;
import com.yammer.dropwizard.util.Duration;

public class TwoNodesBigSyncTest extends ClusterTestBase
{
    // use ports that differ from other tests, just to minimize chance of
    // collision
    private final static int TEST_PORT1 = 9020;
    private final static int TEST_PORT2 = 9021;

    private final static int ENTRIES = 400;
    
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
        final Duration shutdownDelay = Duration.milliseconds(100L);
        BasicTSServiceConfigForDW serviceConfig1 = createNodeConfig("fullStack2sync_1", true, TEST_PORT1, clusterConfig)
               .setShutdownGracePeriod(shutdownDelay)
               .setSyncGracePeriod(new TimeSpan("1s"));
        // all nodes need same (or at least similar enough) cluster config:
        final long START_TIME = 200L;
        final TimeMasterForClusterTesting timeMaster = new TimeMasterForClusterTesting(START_TIME);
        // important: last argument 'true' so that background sync thread gets started
        StoreForTests service1 = StoreForTests.createTestService(serviceConfig1, timeMaster, true);
        BasicTSServiceConfigForDW serviceConfig2 = createNodeConfig("fullStack2sync_2", true, TEST_PORT2, clusterConfig)
               .setShutdownGracePeriod(shutdownDelay);
        serviceConfig2.getServiceConfig().cluster = clusterConfig;
        StoreForTests service2 = StoreForTests.createTestService(serviceConfig2, timeMaster, true);

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
            BasicTSClient client = new AHCBasedClientBootstrapper(clientConfig)
                .addNode(endpoint1)
                .addNode(endpoint2)
                .buildAndInitCompletely(5);
            /* at this point we can let server complete its initial startup,
             * which may include cleanup (test mode usually adds something like
             * 1 msec virtual sleep)
             */
            timeMaster.advanceCurrentTimeMillis(1L);
            Random rnd = new Random(1);
        
            // and start sending stuff!
            for (int i = 0; i < ENTRIES; ++i) {
                final BasicTSKey key = generateKey(rnd, i);
                byte[] data = generateData(rnd);
                PutOperationResult put = client.putContent(clientConfig, key, data);
                assertEquals(put.getSuccessCount(), 1);
                // advance a bit every now and then
                if ((i % 3) == 1) {
                    timeMaster.advanceCurrentTimeMillis(1L);
                }
            }
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

            // and loop for a bit, so that syncing occurs
            for (int i = 0; true; ++i) {
                long entries = service2.getEntryStore().getEntryCount();
                if (entries == ENTRIES) {
                    break;
                }
                if (i > 90) {
                    fail("Failed to sync after "+i+" rounds; entries: "+entries);
                }
                timeMaster.advanceCurrentTimeMillis(15000L);
                Thread.sleep(10L);
            }

            // and then, verify that it is all there, in correct shape
            rnd = new Random(1);
            for (int i = 0; i < ENTRIES; ++i) {
                verifyEntry(i, rnd, service1, service2);
            }
            
        } finally {
            // and That's All, Folks!
            service1.prepareForStop();
            service2.prepareForStop();
            Thread.sleep(10L);
            service1.stop();
            service2.stop();
        }
        try { Thread.sleep(20L); } catch (InterruptedException e) { }
        service1.waitForStopped();
        service2.waitForStopped();
        
    }

    protected void verifyEntry(int index, Random rnd,
            StoreForTests store1, StoreForTests store2) throws Exception
    {
        final BasicTSKey key = generateKey(rnd, index);
        if (store1.getEntryStore().findEntry(key.asStorableKey()) == null) {
            fail("Entry #"+index+"/"+ENTRIES+" missing from original store");
        }
        Storable copy = store2.getEntryStore().findEntry(key.asStorableKey());
        if (copy == null) {
            fail("Entry #"+index+"/"+ENTRIES+" missing from destination store");
        }
        // plus verify contents
        byte[] data = generateData(rnd);
        if (data.length != copy.getOriginalLength()) {
            fail("Entry #"+index+"/"+ENTRIES+" has wrong length ("+copy.getOriginalLength()+"), expect "+data.length);
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
