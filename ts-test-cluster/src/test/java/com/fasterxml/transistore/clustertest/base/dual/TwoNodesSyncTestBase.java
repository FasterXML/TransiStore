package com.fasterxml.transistore.clustertest.base.dual;

import java.io.*;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.skife.config.TimeSpan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.StoreOperationSource;

import io.dropwizard.util.Duration;

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
import com.fasterxml.transistore.service.BasicTSEntry;

public abstract class TwoNodesSyncTestBase extends ClusterTestBase
{
    final static int PORT_BASE = PORT_BASE_DUAL + PORT_DELTA_SYNC;

    // use ports that differ from other tests, just to minimize chance of
    // collision
    private final static int TEST_PORT1 = PORT_BASE+0;
    private final static int TEST_PORT2 = PORT_BASE+1;

    private final IpAndPort endpoint1 = new IpAndPort("localhost:"+TEST_PORT1);
    private final IpAndPort endpoint2 = new IpAndPort("localhost:"+TEST_PORT2);
    
    /**
     * Would be awesome to run with higher number -- test should work for
     * about any number -- but builds get progressively slower. Hence a
     * compromise value seen here.
     */
    private final static int ENTRIES = 1700;

    private final Logger LOG = LoggerFactory.getLogger(getClass());

    /**
     * We need a smaller test for verifying that handling of time-to-live is done
     * in a way that does not "resurrect" almost expired content ([Issue#32]).
     */
    public void testTTLWithSync() throws Exception
    {
        final int ORIG_MAX_TTL_SECS = 3600;
        
        initTestLogging();
        ClusterConfig clusterConfig = twoNodeClusterConfig(endpoint1, endpoint2, 360);
        BasicTSServiceConfigForDW serviceConfig1 = createTwoNodeConfig("fullStack2syncTTL_1",
                TEST_PORT1, clusterConfig);

        final long START_TIME = 200L;
        final TimeMasterForClusterTesting timeMaster = new TimeMasterForClusterTesting(START_TIME);
        // important: last argument 'true' so that background sync thread gets started
        StoreForTests store1 = StoreForTests.createTestService(serviceConfig1, timeMaster, RunMode.TEST_FULL);
        BasicTSServiceConfigForDW serviceConfig2 = createTwoNodeConfig("fullStack2syncTTL_2",
                TEST_PORT2, clusterConfig);
        StoreForTests store2 = StoreForTests.createTestService(serviceConfig2, timeMaster, RunMode.TEST_FULL);

        // but only start first one
        startServices(store1);

        try {
            // at this point we can let server complete its initial startup,
            // which may include cleanup
            timeMaster.advanceCurrentTimeMillis(1L);

            final BasicTSKey KEY = contentKey("testSimple2/syncWithTTL");
            byte[] data = "Tiny data".getBytes("UTF-8");

            // max-TTL of 1 hour, to begin with
            final TimeSpan MAX_TTL = new TimeSpan(ORIG_MAX_TTL_SECS, TimeUnit.SECONDS);
            
            FakeHttpResponse response = new FakeHttpResponse();
            final int inputHash = ChecksumUtil.calcChecksum(data);
            store1.getStoreHandler().putEntry(new FakeHttpRequest(), response,
                    KEY, inputHash, new ByteArrayInputStream(data),
                    null, MAX_TTL, null);
            assertEquals(200, response.getStatus());

            // and should now have all the entries in the first store
            long count = entryCount(store1.getEntryStore());
            if (1L != count) {
                fail("Store 1 should have 1 entry; has "+count);
            }

            // Then the second phase; start up second store, let things sync.
            // But first: move time enough to notice, by 10 minutes
            timeMaster.advanceCurrentTimeMillis(10 * 60 * 1000L);
            
            // So the highest possible TTL, after sync, should be this:
            final int MAX_TTL_SECS_APPROXIMATE = ORIG_MAX_TTL_SECS - (10 * 60);
            
            startServices(store2);
            count = entryCount(store2.getEntryStore());
            if (0L != count) {
                fail("Store 2 should not yet have entries; has "+count);
            }

            // Looks like there's lots of startup overhead here, so need to set to surprisingly high value
            for (int i = 0; i < 500; ++i) { // was 50
                if (entryCount(store2.getEntryStore()) > 0) {
                    break;
                }
                Thread.sleep(10L);
                timeMaster.advanceCurrentTimeMillis(50L);
            }
            // at which point second node should have the contents...
            count = entryCount(store2.getEntryStore());
            if (1L != count) {
                fail("Store 2 should now have 1 entry; has "+count);
            }

            // and we can verify max-TTL setting
            Storable raw = store2.getEntryStore().findEntry(StoreOperationSource.ADMIN_TOOL,
                    null, KEY.asStorableKey());
            assertNotNull(raw);

            BasicTSEntry entry = contentEntry(raw);
            int maxTTLSecs = entry.getMaxTTLSecs();
            
            /* Ok: first things first -- the original bug was that maxTTL was returned unmodified,
             * so let's verify this is not occuring.
             */
            if (maxTTLSecs >= ORIG_MAX_TTL_SECS) {
                fail("MaxTTL returned as "+maxTTLSecs+", unmodified: this is wrong");
            }
            // But the exact value is not really known; we need to check approximate range
            if (maxTTLSecs > MAX_TTL_SECS_APPROXIMATE) {
                fail("MaxTTL returned as "+maxTTLSecs+", while not same as original, should not exceed estimate of "+MAX_TTL_SECS_APPROXIMATE);
            }
            
            // and then again, should not fall below minimum dictated by time now:
            timeMaster.advanceCurrentTimeMillis(1000); // to avoid rounding problems
            final int LOWEST_MAX_TTL = ORIG_MAX_TTL_SECS - (int) ((timeMaster.currentTimeMillis() - START_TIME) / 1000);

            if (maxTTLSecs < LOWEST_MAX_TTL) {
                fail("MaxTTL returned as "+maxTTLSecs+", which is lower than minimum we should get, "+LOWEST_MAX_TTL);
            }
            
        } finally {
            // and That's All, Folks!
            store1.prepareForStop();
            store2.prepareForStop();
            try { Thread.sleep(10L); } catch (InterruptedException e) { }
            store1._stop();
            store2._stop();
            try { Thread.sleep(20L); } catch (InterruptedException e) { }
        }
        store1.waitForStopped();
        store2.waitForStopped();
    }
    
    /* Let's test a sizable dataset, uploading content into just one node,
     * and then let synchronization happen. And once done (by counts),
     * verify that contents are equal.
     */
    public void testBigSync() throws Exception
    {
        initTestLogging();
        ClusterConfig clusterConfig = twoNodeClusterConfig(endpoint1, endpoint2, 360);
        BasicTSServiceConfigForDW serviceConfig1 = createTwoNodeConfig("fullStack2syncBig_1",
                TEST_PORT1, clusterConfig);
        
        // all nodes need same (or at least similar enough) cluster config
        final long START_TIME = 200L;
        final TimeMasterForClusterTesting timeMaster = new TimeMasterForClusterTesting(START_TIME);
        // important: last argument 'true' so that background sync thread gets started
        StoreForTests service1 = StoreForTests.createTestService(serviceConfig1, timeMaster, RunMode.TEST_FULL);
        BasicTSServiceConfigForDW serviceConfig2 = createTwoNodeConfig("fullStack2syncBig_2",
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
                    PutOperationResult put = client.putContent(null, key, data)
                            .completeOptimally()
                            .finish();
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
            assertEquals(ENTRIES, entryCount(service1.getEntryStore()));

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
                long entries = entryCount(service2.getEntryStore());
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
            try { Thread.sleep(20L); } catch (InterruptedException e) { }
        }
        service1.waitForStopped();
        service2.waitForStopped();
    }

    protected BasicTSServiceConfigForDW createTwoNodeConfig(String name, int port, ClusterConfig cluster)
        throws IOException
    {
        final Duration shutdownDelay = Duration.milliseconds(100L);
        return createNodeConfig(name, true, port, cluster)
                .overrideShutdownGracePeriod(shutdownDelay)
                .overrideSyncGracePeriod(new TimeSpan("1s"))
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
        // 16-Apr-2014, tatu: Also, ensure that r(eplica) flag is correctly set
        if (!copy.isReplicated()) {
            fail("Entry #"+index+" not mark as replicated, should be");
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
