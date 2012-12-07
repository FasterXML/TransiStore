package com.fasterxml.transistore.dw;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.junit.Assert;

import com.fasterxml.clustermate.jaxrs.StoreResource;
import com.fasterxml.clustermate.service.msg.PutResponse;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.dw.util.*;
import com.fasterxml.transistore.service.cfg.BasicTSServiceConfig;


public class MediumFileTest extends JaxrsStoreTestBase
{
    @Override
    public void setUp() {
        initTestLogging();
    }

    // Test to use GZIP
    public void testMediumFile() throws IOException
    {
        final long startTime = 1234L;
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(startTime);
		
        StoreResource<BasicTSKey, StoredEntry<BasicTSKey>> resource = createResource("medium", timeMaster, true);
        // ensure it'll use gzip compression
        int origSize = new BasicTSServiceConfig().storeConfig.maxUncompressedSizeForGZIP - 100;
        final String BIG_STRING = biggerCompressibleData(origSize);
        final byte[] BIG_DATA = BIG_STRING.getBytes("UTF-8");
        
        // ok: assume empty Entity Store
        StorableStore entries = resource.getStores().getEntryStore();
        assertEquals(0, entries.getEntryCount());
        
        // then try to find bogus entry; make sure to use a slash...
        final BasicTSKey INTERNAL_KEY1 = contentKey("data/medium/1");

        FakeHttpResponse response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        assertEquals(404, response.getStatus());

        // then try adding said entry
        response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response,
                INTERNAL_KEY1, calcChecksum(BIG_DATA),
                new ByteArrayInputStream(BIG_DATA), null, null, null);
        assertEquals(200, response.getStatus());
        // verify we got a file...
        assertSame(PutResponse.class, response.getEntity().getClass());
        PutResponse<?> presp = (PutResponse<?>) response.getEntity();

        assertFalse(presp.inlined);

        // can we count on this getting updated? Seems to be, FWIW
        assertEquals(1, entries.getEntryCount());

        // Ok. Then, we should also be able to fetch it, right?
        response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        assertEquals(200, response.getStatus());

        // big enough, should be backed by a real file...
        assertTrue(response.hasStreamingContent());
        assertTrue(response.hasFile());
        byte[] data = collectOutput(response);
        assertEquals(BIG_DATA.length, data.length);
        Assert.assertArrayEquals(BIG_DATA, data);

        // and more fundamentally, verify store had it:
        StoredEntry<BasicTSKey> entry = rawToEntry(entries.findEntry(INTERNAL_KEY1.asStorableKey()));
        assertNotNull(entry);
        assertTrue(entry.hasExternalData());
        assertFalse(entry.hasInlineData());
        assertEquals(Compression.GZIP, entry.getCompression());
        assertEquals(BIG_DATA.length, entry.getActualUncompressedLength());
        // compressed, should be smaller...
        assertTrue("Should be compressed; size="+entry.getActualUncompressedLength()+"; storage-size="+entry.getStorageLength(),
                entry.getActualUncompressedLength() > entry.getStorageLength());
        assertEquals(startTime, entry.getCreationTime());
        assertEquals(startTime, entry.getLastModifiedTime());
        assertEquals(startTime, resource.getStores().getLastAccessStore().findLastAccessTime(entry));

        entries.stop();
    }
}
