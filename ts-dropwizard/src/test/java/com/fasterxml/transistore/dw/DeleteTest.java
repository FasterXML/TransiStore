package com.fasterxml.transistore.dw;

import java.io.ByteArrayInputStream;
import java.util.List;

import com.fasterxml.clustermate.jaxrs.StoreResource;
import com.fasterxml.clustermate.service.LastAccessUpdateMethod;
import com.fasterxml.clustermate.service.msg.DeleteResponse;
import com.fasterxml.clustermate.service.msg.PutResponse;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.storemate.store.AdminStorableStore;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.dw.util.*;


/**
 * Unit tests to verify basic functioning of DELETE operation
 */
public class DeleteTest extends JaxrsStoreTestBase
{
    @Override
    public void setUp() {
        initTestLogging();
    }

    public void testCreateDeleteTwo() throws Exception
    {
        long startTime = 1234L;
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(startTime);

        StoreResource<BasicTSKey, StoredEntry<BasicTSKey>> resource = createResource("delete", timeMaster, true);
        final String DATA1_STR = "bit of data, very very short";
        final String DATA2_STR = biggerCompressibleData(29000);
        final byte[] DATA1 = DATA1_STR.getBytes("UTF-8");
        final byte[] DATA2 = DATA2_STR.getBytes("UTF-8");

        // ok: assume empty Entity Store
        StorableStore entries = resource.getStores().getEntryStore();
        assertEquals(0, entries.getEntryCount());

        final String KEY1 = "data/small/1";
        final BasicTSKey INTERNAL_KEY1 = contentKey(KEY1);
        final String KEY2 = "data/bigger-2";
        final BasicTSKey INTERNAL_KEY2 = contentKey(KEY2);

        // first: create 2 entries:
        FakeHttpResponse response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response,
                INTERNAL_KEY1, calcChecksum(DATA1),
                new ByteArrayInputStream(DATA1), null, null, null);
        assertEquals(200, response.getStatus());
        resource.getHandler().putEntry(new FakeHttpRequest(), response,
                INTERNAL_KEY2, calcChecksum(DATA2),
                new ByteArrayInputStream(DATA2), null, null, null);
        assertEquals(200, response.getStatus());
        assertEquals(2, entries.getEntryCount());

        // then verify we can find them:
        response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY2);
        assertEquals(200, response.getStatus());
        assertTrue(response.hasStreamingContent());

        response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        assertEquals(200, response.getStatus());
        assertTrue(response.hasStreamingContent());
        // access does update timestamp
        assertEquals(startTime, resource.getStores().getLastAccessStore().findLastAccessTime(
                INTERNAL_KEY1, LastAccessUpdateMethod.INDIVIDUAL));

        // then try DELETEing second entry first:
        final long deleteTime = timeMaster.advanceCurrentTimeMillis(5000L).currentTimeMillis();
        
        BasicTSKey deleteKey = contentKey(KEY2);
        resource.getHandler().removeEntry(new FakeHttpRequest(), response, deleteKey);

        assertEquals(200, response.getStatus());
        assertSame(DeleteResponse.class, response.getEntity().getClass());
        DeleteResponse<?> dr = (DeleteResponse<?>) response.getEntity();
        assertEquals(deleteKey.toString(), dr.key);
        assertEquals(startTime, dr.creationTime);

        // count won't change, since there's tombstone:
        assertEquals(2, entries.getEntryCount());

        // but shouldn't be able to find it any more; 204 indicates this
        response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY2);
        assertEquals(204, response.getStatus());
        // even though store has the entry
        Storable rawEntry = entries.findEntry(INTERNAL_KEY2.asStorableKey());
        assertNotNull(rawEntry);

        assertTrue(rawEntry.isDeleted());

        StoredEntry<BasicTSKey> entry = rawToEntry(rawEntry);
        
        // important: creationTime does NOT change
        assertEquals(startTime, entry.getCreationTime());
        // but last-modified should
        assertEquals(Long.toHexString(deleteTime), Long.toHexString(entry.getLastModifiedTime()));

        // but the other entry is there
        entry = rawToEntry(entries.findEntry(INTERNAL_KEY1.asStorableKey()));
        assertNotNull(entry);
        assertFalse(entry.isDeleted());
        response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        assertEquals(200, response.getStatus());

        // But content dump should give two non-deleted entries...
        List<Storable> e = ((AdminStorableStore) entries).dumpEntries(10, true);
        assertEquals(2, e.size());

        // ok to "re-delete"...
        response = new FakeHttpResponse();
        resource.getHandler().removeEntry(new FakeHttpRequest(), response,
                contentKey(KEY2));
        assertEquals(200, response.getStatus());
        assertSame(DeleteResponse.class, response.getEntity().getClass());

        // then, delete the other entry as well
        response = new FakeHttpResponse();
        deleteKey = contentKey(KEY1);
        resource.getHandler().removeEntry(new FakeHttpRequest(), response,
                deleteKey);
        assertEquals(200, response.getStatus());
        assertSame(DeleteResponse.class, response.getEntity().getClass());
        dr = (DeleteResponse<?>) response.getEntity();
        assertEquals(deleteKey.toString(), dr.key);
        assertEquals(startTime, dr.creationTime);

        assertEquals(2, entries.getEntryCount());
        response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        assertEquals(204, response.getStatus());

        // clean up:
        resource.getStores().stop();
    }
    
    /**
     * Test that verifies that an attempt to re-create a deleted resource
     * fails.
     */
    public void testDeleteTryRecreate() throws Exception
    {
        long startTime = 1234L;
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(startTime);
        StoreResource<BasicTSKey, StoredEntry<BasicTSKey>> resource
            = createResource("deleteRecreate", timeMaster, true);
        final byte[] DATA1 = "bit of data".getBytes("UTF-8");

        StorableStore entries = resource.getStores().getEntryStore();
        assertEquals(0, entries.getEntryCount());

        final String KEY1 = "data/small/1";
        final BasicTSKey INTERNAL_KEY1 = contentKey(KEY1);

        // first: create the entry
        FakeHttpResponse response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response,
                INTERNAL_KEY1, calcChecksum(DATA1),
                new ByteArrayInputStream(DATA1), null, null, null);
        assertEquals(200, response.getStatus());
        assertEquals(1, entries.getEntryCount());

        // then DELETE it
        timeMaster.advanceCurrentTimeMillis(10L);
        resource.getHandler().removeEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        assertEquals(200, response.getStatus());
        assertSame(DeleteResponse.class, response.getEntity().getClass());
        DeleteResponse<?> dr = (DeleteResponse<?>) response.getEntity();
        assertEquals(INTERNAL_KEY1.toString(), dr.key);
        assertEquals(startTime, dr.creationTime);
        // count won't change, since there's tombstone:
        assertEquals(1, entries.getEntryCount());

        // but then an attempt to "re-PUT" entry must fail with 410 (not conflict)
        response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response,
                INTERNAL_KEY1, calcChecksum(DATA1),
                new ByteArrayInputStream(DATA1), null, null, null);
        assertEquals(410, response.getStatus());
        PutResponse<?> pr = (PutResponse<?>) response.getEntity();
        verifyMessage("Failed PUT: trying to recreate", pr.message);
    }
}
