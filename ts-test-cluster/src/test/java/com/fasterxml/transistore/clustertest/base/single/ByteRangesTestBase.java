package com.fasterxml.transistore.clustertest.base.single;

import static org.junit.Assert.assertArrayEquals;

import java.util.Arrays;

import com.fasterxml.storemate.shared.ByteRange;
import com.fasterxml.storemate.shared.IpAndPort;

import com.fasterxml.clustermate.client.operation.PutOperationResult;
import com.fasterxml.clustermate.dw.RunMode;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.client.*;
import com.fasterxml.transistore.clustertest.ClusterTestBase;
import com.fasterxml.transistore.clustertest.StoreForTests;
import com.fasterxml.transistore.clustertest.util.TimeMasterForClusterTesting;
import com.fasterxml.transistore.dw.BasicTSServiceConfigForDW;

/**
 * Tests to verify that we can use byte ranges to access part of content.
 */
public abstract class ByteRangesTestBase extends ClusterTestBase
{
    final static int PORT_BASE = PORT_BASE_SINGLE + PORT_DELTA_RANGE;
    
    public void testSimpleSingleNode() throws Exception
    {
        initTestLogging(); // reduce noise
        
        BasicTSServiceConfigForDW serviceConfig = createSingleNodeConfig("fullStack1ByteRanges", true, PORT_BASE);
        // false -> don't bother with full init of background tasks:
        StoreForTests service = StoreForTests.createTestService(serviceConfig,
                new TimeMasterForClusterTesting(100L), RunMode.TEST_MINIMAL);
        startServices(service);

        // Ok: now, let's try doing some basic stuff
        BasicTSClientConfig clientConfig = new BasicTSClientConfigBuilder()
	        .setOptimalOks(1)
	        .setMaxOks(1)
	        .build();
        BasicTSClient client = createClient(clientConfig, new IpAndPort("http", "localhost", PORT_BASE));

        // // // Small content: inlined, not-compressed
        
        // Add test content
        final BasicTSKey SMALL_KEY = contentKey("testRange-item1");
        final byte[] SMALL_CONTENT = "Something pretty small to store, not compressed".getBytes("UTF-8");
        PutOperationResult result = client.putContent(null, SMALL_KEY, SMALL_CONTENT).completeOptimally();
        assertTrue(result.succeededOptimally());

        // first: verify that we can do GET, but not find the entry:
        int OFFSET = 3;
        int LENGTH = 11;
        byte[] data = client.getPartialContentAsBytes(null, SMALL_KEY, new ByteRange(OFFSET, LENGTH));
        assertNotNull(data);
        if (data.length != LENGTH) {
        	assertEquals("Range failed for content of "+SMALL_CONTENT.length+" bytes", LENGTH, data.length);
        }
        byte[] exp = Arrays.copyOfRange(SMALL_CONTENT, OFFSET, OFFSET+LENGTH);
        assertArrayEquals(exp, data);

        // And then bit bigger content, to get GZIP handling tested
        OFFSET = 2900;
        LENGTH = 77;
        final BasicTSKey MED_KEY = contentKey("testRange-item2");
        final byte[] MED_CONTENT = biggerSomewhatCompressibleData(3000);
        result = client.putContent(null, MED_KEY, MED_CONTENT).completeOptimally();
        assertTrue(result.succeededOptimally());
        data = client.getPartialContentAsBytes(null, MED_KEY, new ByteRange(OFFSET, LENGTH));
        assertNotNull(data);
        if (data.length != LENGTH) {
        	assertEquals("Range failed for content of "+MED_CONTENT.length+" bytes", LENGTH, data.length);
        }
        exp = Arrays.copyOfRange(MED_CONTENT, OFFSET, OFFSET+LENGTH);
        assertArrayEquals(exp, data);
        
        // and finally, "big" content
        final BasicTSKey BIG_KEY = contentKey("testRange-item3");
        final byte[] BIG_CONTENT = biggerSomewhatCompressibleData(200 * 1024); // 200k
        OFFSET = 128456;
        LENGTH = 5600;
        result = client.putContent(null, BIG_KEY, BIG_CONTENT).completeOptimally();
        assertTrue(result.succeededOptimally());
        data = client.getPartialContentAsBytes(null, BIG_KEY, new ByteRange(OFFSET, LENGTH));
        assertNotNull(data);
        if (data.length != LENGTH) {
        	assertEquals("Range failed for content of "+BIG_CONTENT.length+" bytes", LENGTH, data.length);
        }
        exp = Arrays.copyOfRange(BIG_CONTENT, OFFSET, OFFSET+LENGTH);
        assertArrayEquals(exp, data);
        
        // and That's All, Folks!

        service._stop();
        Thread.yield();
        service.waitForStopped();
    }
}