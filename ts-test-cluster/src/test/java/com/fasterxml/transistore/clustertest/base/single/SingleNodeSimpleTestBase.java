package com.fasterxml.transistore.clustertest.base.single;

import java.io.*;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;

import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.store.StoreConfig;

import com.fasterxml.clustermate.client.operation.DeleteOperationResult;
import com.fasterxml.clustermate.client.operation.PutOperationResult;
import com.fasterxml.clustermate.dw.RunMode;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.client.*;
import com.fasterxml.transistore.clustertest.ClusterTestBase;
import com.fasterxml.transistore.clustertest.StoreForTests;
import com.fasterxml.transistore.clustertest.util.TimeMasterForClusterTesting;
import com.fasterxml.transistore.dw.BasicTSServiceConfigForDW;

/**
 * Set of simple (CRUD) tests for a single-node system, running within
 * JVM.
 */
public abstract class SingleNodeSimpleTestBase extends ClusterTestBase
{
    final static int PORT_BASE = PORT_BASE_SINGLE + PORT_DELTA_SIMPLE;

    final static int PORT_1 = PORT_BASE + 0;
    final static int PORT_2 = PORT_BASE + 1;
    
    final static int MAX_PAYLOAD_IN_MEMORY = StoreConfig.DEFAULT_MIN_PAYLOAD_FOR_STREAMING-1;
    
    public void testSimpleSingleNode() throws Exception
    {
        initTestLogging(); // reduce noise
        
        BasicTSServiceConfigForDW serviceConfig = createSingleNodeConfig("fullStack1", true, PORT_1);
        // false -> don't bother with full init of background tasks:
        StoreForTests service = StoreForTests.createTestService(serviceConfig,
                new TimeMasterForClusterTesting(100L), RunMode.TEST_MINIMAL);
        startServices(service);

        // Ok: now, let's try doing some basic stuff
        BasicTSClientConfig clientConfig = new BasicTSClientConfigBuilder()
                .setGetCallTimeoutMsecs(2500L)
                .setOptimalOks(1)
                .setMaxOks(1)
                .setAllowRetries(false) // no retries!
                .build();
        BasicTSClient client = createClient(clientConfig, new IpAndPort("http", "localhost", PORT_1));

        /* 02-Nov-2012, tatu: Minor twist -- use non-ASCII character(s) in
         *   there to trigger possible encoding probs. Ditto for embedded slash.
         */
        final BasicTSKey KEY = contentKey("test/Stuff-\u00A9");

        try {
            // first: verify that we can do GET, but not find the entry:
            byte[] data = client.getContentAsBytes(clientConfig, KEY);
            assertNull("Should not yet have entry", data);
    
            // Then add said content
            final int LENGTH = 12000;
            final byte[] CONTENT = new byte[LENGTH];
            Arrays.fill(CONTENT, (byte) 0xAC);
            PutOperationResult result = client.putContent(clientConfig, null, KEY, CONTENT);
            assertTrue(result.succeededOptimally());

            // find it; both with GET and HEAD
            data = client.getContentAsBytes(clientConfig, KEY);
            assertNotNull("Should now have the data", data);
            assertArrayEquals(CONTENT, data);
            
            long len = client.getContentLength(clientConfig, KEY);
            assertEquals(LENGTH, len);
    
            // delete:
            DeleteOperationResult del = client.deleteContent(clientConfig, KEY);
            assertTrue(del.succeededMinimally());
            assertTrue(del.succeededOptimally());
    
            // after which content ... is no more:
            data = client.getContentAsBytes(clientConfig, KEY);
            assertNotNull("Should not have the data after DELETE", data);
        } finally {
            // and That's All, Folks!
            service._stop();
            service.waitForStopped();
        }
    }

    /**
     * Alternate test that uses backing file as the source; needed since
     * File as input source differs from byte arrays
     */
    public void testSingleNodeWithFile() throws Exception
    {
        initTestLogging(); // reduce noise
        BasicTSServiceConfigForDW serviceConfig = createSingleNodeConfig("fullStack1F", true, PORT_1);
        StoreForTests service = StoreForTests.createTestService(serviceConfig,
                new TimeMasterForClusterTesting(100L), RunMode.TEST_MINIMAL);
        startServices(service);

        BasicTSClientConfig clientConfig = new BasicTSClientConfigBuilder()
                .setOptimalOks(1).setMaxOks(1)
                .setAllowRetries(false) // no retries!
                .build();
        BasicTSClient client = createClient(clientConfig, new IpAndPort("http", "localhost", PORT_1));
        final BasicTSKey KEY = contentKey("testSimple-file");

        try {
            // first: verify that we can do GET, but not find the entry:
            byte[] data = client.getContentAsBytes(clientConfig, KEY);
            assertNull("Should not yet have entry", data);
            // Then add said content
            int origSize = MAX_PAYLOAD_IN_MEMORY + 100;
            final byte[] CONTENT = biggerSomewhatCompressibleData(origSize);
            File file = File.createTempFile("test", "test-data.txt");
            file.deleteOnExit();
            FileOutputStream fout = new FileOutputStream(file);
            fout.write(CONTENT);
            fout.close();
    
            PutOperationResult result = client.putContent(clientConfig, null, KEY, file);
            if (!result.succeededOptimally()) {
                fail("PUT failed, with: "+result.getFirstFail());
            }
    
            // find it; both with GET and HEAD
            data = client.getContentAsBytes(clientConfig, KEY);
            assertNotNull("Should now have the data", data);
            assertArrayEquals(CONTENT, data);
    
            long len = client.getContentLength(clientConfig, KEY);
            assertEquals(data.length, len);
    
            // delete:
            DeleteOperationResult del = client.deleteContent(clientConfig, KEY);
            assertTrue(del.succeededMinimally());
            assertTrue(del.succeededOptimally());
    
            // after which content ... is no more:
            data = client.getContentAsBytes(clientConfig, KEY);
            assertNotNull("Should not have the data after DELETE", data);
            // and That's All, Folks!
        } finally {
            service._stop();
            service.waitForStopped();
        }        
    }
}
