package com.fasterxml.transistore.dw;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.dw.util.FakeHttpRequest;
import com.fasterxml.transistore.dw.util.FakeHttpResponse;
import com.fasterxml.transistore.dw.util.StoreResourceForTests;
import com.fasterxml.transistore.dw.util.TimeMasterForSimpleTesting;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.ContentType;
import com.fasterxml.clustermate.api.KeyRange;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.sync.SyncHandler;
import com.fasterxml.clustermate.service.sync.SyncListResponse;

/**
 * Test case(s) to verify that we can handle basic pull list request
 */
public class SyncListTest extends JaxrsStoreTestBase
{
    @Override
    public void setUp() {
        initTestLogging();
    }

    public void testSimpleSyncList() throws IOException
    {
        final long creationTime = 1234L;
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(creationTime);

        StoreResourceForTests<BasicTSKey, StoredEntry<BasicTSKey>> resource = createResource("syncSimple", timeMaster, true);

        SyncHandler<BasicTSKey, StoredEntry<BasicTSKey>> syncH = new SyncHandler<BasicTSKey, StoredEntry<BasicTSKey>>
            (resource._stuff, resource.getStores(), resource.getCluster());
        
        // First, add an entry:
        StorableStore entries = resource.getStores().getEntryStore();
        final BasicTSKey KEY1 = contentKey("data/entry/1");
        final byte[] SMALL_DATA = "Some data that we want to store -- small, gets inlined...".getBytes("UTF-8");

        FakeHttpResponse response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, KEY1);
        response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response, KEY1,
                calcChecksum(SMALL_DATA), new ByteArrayInputStream(SMALL_DATA), null, null, null);
        assertEquals(200, response.getStatus());
        assertEquals(1, entries.getEntryCount());

        // then advance time, try to access change list
        timeMaster.advanceCurrentTimeMillis(30000L);

        FakeHttpRequest syncReq = new FakeHttpRequest();
        final KeyRange localRange = resource.getKeyRange();
        syncReq.addQueryParam(ClusterMateConstants.QUERY_PARAM_KEYRANGE_START, ""+localRange.getStart());
        syncReq.addQueryParam(ClusterMateConstants.QUERY_PARAM_KEYRANGE_LENGTH, ""+localRange.getLength());
        // JSON or Smile? Either should be fine...
        syncReq.addHeader(ClusterMateConstants.HTTP_HEADER_ACCEPT, ContentType.SMILE.toString());
        
        response = new FakeHttpResponse();
        syncH.listEntries(syncReq, response, creationTime, null);
        assertTrue(response.hasStreamingContent());
        assertEquals(200, response.getStatus());
        assertEquals(ContentType.SMILE.toString(), response.getContentType());
        byte[] data = response.getStreamingContentAsBytes();

        SyncListResponse<?> syncList = resource._stuff.smileReader(SyncListResponse.class).readValue(data);
        assertNotNull(syncList);
        assertNull(syncList.message);
        assertNotNull(syncList.entries);
        assertEquals(1, syncList.entries.size());
    }

    /**
     * Test to verify that in case of large block of entries with same last-mod
     * timestamp, max entries to list must be relaxed so that caller
     * can advance.
     */
    public void testLargerSyncList() throws IOException
    {
        final long creationTime = 1234L;
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(creationTime);

        StoreResourceForTests<BasicTSKey, StoredEntry<BasicTSKey>> resource = createResource("syncLarger", timeMaster, true);

        // set handler's "max-to-list" to 1, less than what we need later on
        SyncHandler<BasicTSKey, StoredEntry<BasicTSKey>> syncH =
                new SyncHandler<BasicTSKey, StoredEntry<BasicTSKey>>(resource._stuff,
                resource.getStores(), resource.getCluster(), 1);
        
        // First, add an entry:
        StorableStore entries = resource.getStores().getEntryStore();
        final BasicTSKey KEY1 = contentKey("data/entry/1");
        final BasicTSKey KEY2 = contentKey("data/entry/2");
        final BasicTSKey KEY3 = contentKey("data/entry/3");
        final byte[] SMALL_DATA = "Some data that we want to store -- small, gets inlined...".getBytes("UTF-8");
        final int hash = calcChecksum(SMALL_DATA);

        FakeHttpResponse response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response, KEY1,
                hash, new ByteArrayInputStream(SMALL_DATA), null, null, null);
        assertEquals(200, response.getStatus());
        response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response, KEY2,
                hash, new ByteArrayInputStream(SMALL_DATA), null, null, null);
        assertEquals(200, response.getStatus());
        response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response, KEY3,
                hash, new ByteArrayInputStream(SMALL_DATA), null, null, null);
        assertEquals(200, response.getStatus());
        
        assertEquals(3, entries.getEntryCount());
        timeMaster.advanceCurrentTimeMillis(30000L);

        FakeHttpRequest syncReq = new FakeHttpRequest();
        final KeyRange localRange = resource.getKeyRange();
        syncReq.addQueryParam(ClusterMateConstants.QUERY_PARAM_KEYRANGE_START, ""+localRange.getStart());
        syncReq.addQueryParam(ClusterMateConstants.QUERY_PARAM_KEYRANGE_LENGTH, ""+localRange.getLength());
        syncReq.addHeader(ClusterMateConstants.HTTP_HEADER_ACCEPT, ContentType.SMILE.toString());
        
        response = new FakeHttpResponse();
        syncH.listEntries(syncReq, response, creationTime, null);
        assertTrue(response.hasStreamingContent());
        assertEquals(200, response.getStatus());
        assertEquals(ContentType.SMILE.toString(), response.getContentType());
        byte[] data = response.getStreamingContentAsBytes();

        SyncListResponse<StoredEntry<?>> syncList = resource._stuff.smileReader(SyncListResponse.class).readValue(data);
        assertNotNull(syncList);
        assertNull(syncList.message);
        assertNotNull(syncList.entries);
        // Important: we MUST get all 3, as they have same timestamp
        assertEquals(3, syncList.entries.size());
    }
}
