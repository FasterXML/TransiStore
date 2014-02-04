package com.fasterxml.transistore.clustertest.base.single;

import java.util.List;

import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.StorableKey;

import com.fasterxml.clustermate.api.ListItemType;
import com.fasterxml.clustermate.client.operation.ListOperationResult;
import com.fasterxml.clustermate.client.operation.PutOperationResult;
import com.fasterxml.clustermate.client.operation.StoreEntryLister;
import com.fasterxml.clustermate.dw.RunMode;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.basic.BasicTSListItem;
import com.fasterxml.transistore.client.*;
import com.fasterxml.transistore.clustertest.ClusterTestBase;
import com.fasterxml.transistore.clustertest.StoreForTests;
import com.fasterxml.transistore.clustertest.util.TimeMasterForClusterTesting;
import com.fasterxml.transistore.dw.BasicTSServiceConfigForDW;

public abstract class ListEntriesTestBase extends ClusterTestBase
{
    final static int PORT_BASE = PORT_BASE_SINGLE + PORT_DELTA_LIST;

    final static int PORT_1 = PORT_BASE + 0;
    final static int PORT_2 = PORT_BASE + 1;
    
    private final static String PARTITION1 = "a";
    private final static String PARTITION2 = "b";
    private final static String PARTITION3 = "c";
    
    public void testSimpleIdListing() throws Exception
    {
        initTestLogging(); // reduce noise

        BasicTSServiceConfigForDW serviceConfig = createSingleNodeConfig("fullStack1ListId", true, PORT_1);
        StoreForTests service = StoreForTests.createTestService(serviceConfig,
                new TimeMasterForClusterTesting(100L), RunMode.TEST_MINIMAL); // false -> minimal background tasks
        startServices(service);

        try {
            BasicTSClientConfig clientConfig = new BasicTSClientConfigBuilder()
                 .setOptimalOks(1).setMaxOks(1).build();
            BasicTSClient client = createClient(clientConfig, new IpAndPort("http", "localhost", PORT_1));
    
            // First, set up test data: 5 things to iterate, 3 others
            addEntry(client, PARTITION2, "foo");
            addEntry(client, PARTITION2, "bar");
            addEntry(client, PARTITION2, "dir/abc");
            addEntry(client, PARTITION2, "dir/def");
            addEntry(client, PARTITION2, "zzz");
    
            addEntry(client, null, "foo");
            addEntry(client, PARTITION3, "foo");
            addEntry(client, PARTITION1, "bar");
    
            
            // And then iterate everything under PARTITION 2, first
            //    public final <T> StoreEntryLister<K,T> listContent(K prefix, ListType itemType) throws InterruptedException {
    
            StoreEntryLister<BasicTSKey, StorableKey> lister = client.listContent(null, contentKey(PARTITION2, ""), ListItemType.ids);
    
            ListOperationResult<StorableKey> row = lister.listMore(2);
            if (!row.succeeded()) {
                fail("Failed: "+row.getFirstFail());
            }
            List<StorableKey> ids = row.getItems();
            assertEquals(2, ids.size());
            assertEquals(contentKey(PARTITION2, "bar"), contentKey(ids.get(0)));
            assertEquals(contentKey(PARTITION2, "dir/abc"), contentKey(ids.get(1)));
            
            row = lister.listMore(4);
            assertTrue(row.succeeded());
            ids = row.getItems();
            assertEquals(3, ids.size());
            assertEquals(contentKey(PARTITION2, "dir/def"), contentKey(ids.get(0)));
            assertEquals(contentKey(PARTITION2, "foo"), contentKey(ids.get(1)));
            assertEquals(contentKey(PARTITION2, "zzz"), contentKey(ids.get(2)));
    
            // and then no more entries
            row = lister.listMore(4);
            if (!row.succeeded()) {
                fail("Failed: "+row.getFirstFail());
            }
            assertEquals(0, row.getItems().size());
            
            // // Second scan; now with longer prefix...
    
            lister = client.listContent(null, contentKey(PARTITION2, "dir/"), ListItemType.ids);
            row = lister.listMore(1);
            if (!row.succeeded()) {
                fail("Failed: "+row.getFirstFail());
            }
            assertEquals(1, row.getItems().size());
            assertEquals(contentKey(PARTITION2, "dir/abc"), contentKey(row.getItems().get(0)));
    
            row = lister.listMore(1);
            assertTrue(row.succeeded());
            assertEquals(1, row.getItems().size());
            assertEquals(contentKey(PARTITION2, "dir/def"), contentKey(row.getItems().get(0)));
            row = lister.listMore(1);
            if (!row.succeeded()) {
                fail("Failed: "+row.getFirstFail());
            }
            assertEquals(0, row.getItems().size());
        } finally { 
            service._stop();
            Thread.yield();
            service.waitForStopped();
        }
    }

    public void testFullItemListing() throws Exception
    {
        initTestLogging(); // reduce noise
        
        BasicTSServiceConfigForDW serviceConfig = createSingleNodeConfig("fullStack1ListFullItem", true, PORT_2);
        StoreForTests service = StoreForTests.createTestService(serviceConfig,
                new TimeMasterForClusterTesting(100L), RunMode.TEST_MINIMAL); // false -> minimal background tasks
        startServices(service);

        try {
            BasicTSClientConfig clientConfig = new BasicTSClientConfigBuilder()
                 .setOptimalOks(1).setMaxOks(1).build();
            BasicTSClient client = createClient(clientConfig, new IpAndPort("http", "localhost", PORT_2));
    
            // First, set up test data: 5 things to iterate, 3 others
            addEntry(client, PARTITION2, "foo");
            addEntry(client, PARTITION2, "bar");
            addEntry(client, PARTITION2, "dir/abc");
            addEntry(client, PARTITION2, "dir/def");
            addEntry(client, PARTITION2, "zzz");
    
            addEntry(client, null, "foo");
            addEntry(client, PARTITION3, "foo");
            addEntry(client, PARTITION1, "bar");
            // Also, important: need to list with other types as well; esp. full items
    
            StoreEntryLister<BasicTSKey, BasicTSListItem> listerFull = client.listContent(null, contentKey(PARTITION2, ""),
                    ListItemType.fullEntries);
    
            ListOperationResult<BasicTSListItem> rowFull = listerFull.listMore(2);
            if (!rowFull.succeeded()) {
                fail("Failed: "+rowFull.getFirstFail());
            }
            List<BasicTSListItem> fullItems = rowFull.getItems();
            assertEquals(2, fullItems.size());
            assertEquals(BasicTSListItem.class, fullItems.get(0).getClass());
            assertEquals(BasicTSListItem.class, fullItems.get(1).getClass());
            // should we verify contents?
        } finally {
            service._stop();
            Thread.yield();
            service.waitForStopped();
        }
    }

    private void addEntry(BasicTSClient client, String partition, String path)
        throws Exception
    {
        final BasicTSKey KEY = contentKey(partition, path);
        final byte[] CONTENT = ("Content:"+path).getBytes("UTF-8");
        PutOperationResult result = client.putContent(null, KEY, CONTENT)
                .completeOptimally()
                .finish();
        assertTrue(result.succeededOptimally());
    }
}
