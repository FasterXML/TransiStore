package com.fasterxml.transistore.clustertest.base.single;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;

import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.store.StoreConfig;

import com.fasterxml.clustermate.client.operation.PutOperationResult;
import com.fasterxml.clustermate.dw.RunMode;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.client.*;
import com.fasterxml.transistore.clustertest.ClusterTestBase;
import com.fasterxml.transistore.clustertest.StoreForTests;
import com.fasterxml.transistore.clustertest.util.TimeMasterForClusterTesting;
import com.fasterxml.transistore.dw.BasicTSServiceConfigForDW;

/**
 * Basic test cases for testing expiration of files/data on Vagabond.
 */
public abstract class SingleNodeExpirationTestBase extends ClusterTestBase
{
	final static int MAX_PAYLOAD_IN_MEMORY = StoreConfig.DEFAULT_MIN_PAYLOAD_FOR_STREAMING - 1;

	final static int PORT_BASE = PORT_BASE_SINGLE + PORT_DELTA_EXPIRATION;

	private final static int TEST_PORT1 = PORT_BASE;
     private final static int TEST_PORT2 = PORT_BASE+2;
     private final static int TEST_PORT3 = PORT_BASE+3;

     public void testSimpleExpiration() throws Exception
     {
         initTestLogging(); // reduce noise
         final long START_TIME = 100L;
		final TimeMasterForClusterTesting timeMaster = new TimeMasterForClusterTesting(
				START_TIME);
		BasicTSServiceConfigForDW serviceConfig = createSingleNodeConfig("expire1",
				true, TEST_PORT1);
		// defining the expiry of files to be 2 seconds
		serviceConfig.getServiceConfig().overrideDefaultMaxTTL("2s");
		serviceConfig.getServiceConfig().overrideDefaultSinceAccessTTL("2s");
		// false -> don't bother with full init of background tasks:
		StoreForTests service = StoreForTests.createTestService(serviceConfig,
				timeMaster, RunMode.TEST_FULL);
		startServices(service);

		BasicTSClientConfig clientConfig = _clientConfig();
		BasicTSClient client = createClient(clientConfig, new IpAndPort("http", "localhost", TEST_PORT1));

		final BasicTSKey KEY = contentKey("test/expire/1");

		try {
			// first: verify that we can do GET, but not find the entry:
			byte[] data = client.getContentAsBytes(KEY);
			assertNull("Should not yet have entry", data);

			// Then add said content
			final int LENGTH = 12000;
			final byte[] CONTENT = new byte[LENGTH];
			Arrays.fill(CONTENT, (byte) 0xAC);
			
			PutOperationResult result = client.putContent(null, KEY, CONTENT);
			assertTrue(result.succeededOptimally());

			// find it; both with GET and HEAD
			data = client.getContentAsBytes(KEY);
			
			assertNotNull(data);
			assertEquals(LENGTH, data.length);
			_verifyExpiration(client, timeMaster, KEY);
		} finally {
		    service.prepareForStop();
              try { Thread.sleep(20L); } catch (InterruptedException e) { }
		    service._stop();
		    service.waitForStopped();
		}
	}
	
	/**
	 * Inserts multiple files and tests whether, once they are epxired, they get
	 * deleted or not. THIS ONE PASSES. THE ONE BELOW IT FAILS
	 */
	public void testLoopMultipleFiles() throws Exception
	{
		initTestLogging(); // reduce noise
		final long START_TIME = 100L;
		final TimeMasterForClusterTesting timeMaster = new TimeMasterForClusterTesting(
				START_TIME);
		BasicTSServiceConfigForDW serviceConfig = createSingleNodeConfig("expire3",
				true, TEST_PORT2);
		// defining the expiry of files to be 2 seconds
		serviceConfig.getServiceConfig().overrideDefaultMaxTTL("2s");
		serviceConfig.getServiceConfig().overrideDefaultSinceAccessTTL("2s");
		// false -> don't bother with full init of background tasks:
		StoreForTests service = StoreForTests.createTestService(serviceConfig,
				timeMaster, RunMode.TEST_FULL);
		startServices(service);

          BasicTSClientConfig clientConfig = _clientConfig();
          BasicTSClient client = createClient(clientConfig, new IpAndPort("http", "localhost", TEST_PORT2));

		// System.out.println("key is: " + KEY.toString());
		try {
			ArrayList<BasicTSKey> keys = new ArrayList<BasicTSKey>();
			for (int i = 0; i < 2; i++) {
			    BasicTSKey KEY = contentKey("test/expire/looped-file" + i);
				keys.add(KEY);
				// first: verify that we can do GET, but not find the entry:
				byte[] data = client.getContentAsBytes(clientConfig, KEY);
				assertNull("Should not yet have entry", data);
				// Then add said content
				int origSize = MAX_PAYLOAD_IN_MEMORY + 100;
				final byte[] CONTENT = biggerSomewhatCompressibleData(origSize);
				PutOperationResult result = client.putContent(clientConfig,null,
						KEY, CONTENT);
				if (!result.succeededOptimally()) {
					fail("PUT failed, with: " + result.getFirstFail());
				}

				// find it; both with GET and HEAD
				data = client.getContentAsBytes(clientConfig, KEY);
				assertNotNull("Should now have the data", data);
				assertArrayEquals(CONTENT, data);

				long len = client.getContentLength(clientConfig, KEY);

				assertEquals(data.length, len);
			}
			for (BasicTSKey key : keys) {
	               _verifyExpiration(client, timeMaster, key);
			}
		} finally {
              service.prepareForStop();
              try { Thread.sleep(20L); } catch (InterruptedException e) { }
              service._stop();
              service.waitForStopped();
		}

	}

	/**
	 * * 
	 * Inserts multiple files and tests whether, once they are expired, they get
	 * deleted or not.
	 * 
	 * THIS IS THE TEST THAT FAILS. THE ABOVE ONE PASSES. 
	 * @throws Exception
	 */
	public void testMultipleEntriesFails() throws Exception
	{
		initTestLogging(); // reduce noise
		final long START_TIME = 100L;
		final TimeMasterForClusterTesting timeMaster = new TimeMasterForClusterTesting(START_TIME);
		BasicTSServiceConfigForDW serviceConfig = createSingleNodeConfig("expire4",
				true, TEST_PORT3);
		// defining the expiry of files to be 2 seconds
		serviceConfig.getServiceConfig().overrideDefaultMaxTTL("2s");
		serviceConfig.getServiceConfig().overrideDefaultSinceAccessTTL("2s");
		// false -> don't bother with full init of background tasks:
		StoreForTests service = StoreForTests.createTestService(serviceConfig,
				timeMaster, RunMode.TEST_FULL);
		startServices(service);

          BasicTSClientConfig clientConfig = _clientConfig();
          BasicTSClient client = createClient(clientConfig, new IpAndPort("http", "localhost", TEST_PORT3));

		// System.out.println("key is: " + KEY.toString());
		try {
			ArrayList<BasicTSKey> keys = new ArrayList<BasicTSKey>();
			for (int i = 1; i < 5; i++) {
				BasicTSKey KEY = contentKey("test/expire/multi/file" + i);
				keys.add(KEY);
				// first: verify that we can do GET, but not find the entry:
				byte[] data = client.getContentAsBytes(clientConfig, KEY);
				assertNull("Should not yet have entry", data);
				// Then add said content
				int origSize = MAX_PAYLOAD_IN_MEMORY + 100;
				final byte[] CONTENT = biggerSomewhatCompressibleData(origSize);
				PutOperationResult result = client.putContent(clientConfig, null,
						KEY, CONTENT);
				if (!result.succeededOptimally()) {
					fail("PUT failed, with: " + result.getFirstFail());
				}
				// find it; both with GET and HEAD
				data = client.getContentAsBytes(clientConfig, KEY);
				assertNotNull("Should now have the data", data);
				assertArrayEquals(CONTENT, data);
				assertEquals(data.length, client.getContentLength(clientConfig, KEY));

                    _verifyExpiration(client, timeMaster, KEY);
			}

		} finally {
              service.prepareForStop();
              try { Thread.sleep(20L); } catch (InterruptedException e) { }
              service._stop();
              service.waitForStopped();
		}
	}

     private BasicTSClientConfig _clientConfig() {
         return new BasicTSClientConfigBuilder()
         .setAllowRetries(false) // no retries!
         .setOptimalOks(1).setMaxOks(1).build();
     }
	
     protected void _verifyExpiration(BasicTSClient client,
             TimeMasterForClusterTesting timeMaster, BasicTSKey key)
         throws Exception
     {
         // First: fast forward time by 33 minutes (to make sure that clean up tasks
         // have been completed)
         timeMaster.advanceCurrentTimeMillis(33 * 60 * 1000L);

         // start with bit of sleep to give a chance for expiration to work
         Thread.sleep(30);

         // ... but it may take bit longer
         for (int i = 0; i < 10; ++i) {
             byte[] data = client.getContentAsBytes(key);
              if (data == null) {
                  return;
              }
              timeMaster.advanceCurrentTimeMillis(10 * 60 * 1000L);
              Thread.sleep(100);
         }
         fail("Entry with key '"+key+"' should not exist after expiration");
     }
}
