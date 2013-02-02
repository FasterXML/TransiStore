package com.fasterxml.transistore.service;

import junit.framework.TestCase;

import com.fasterxml.storemate.shared.ByteContainer;

import com.fasterxml.clustermate.service.store.StoredEntry;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.basic.BasicTSKeyConverter;

public class TestEntryMetadata extends TestCase
{
    /* Unit test for checking that various methods of calculating
     * routing hashes give the same answer.
     * This is due to a regression where variant did not hold true.
     */
    public void testHashingForRouting()
    {
        final BasicTSKeyConverter keyConverter = BasicTSKeyConverter.defaultInstance();

        BasicTSKey key = keyConverter.construct("silly-key");
        BasicTSEntry entry = new BasicTSEntry(key, null,
                123L, 10, 100, TSLastAccess.SIMPLE);
        assertEquals("0x"+Integer.toHexString(entry.routingHashUsing(keyConverter)),
                "0x"+Integer.toHexString(keyConverter.routingHashFor(entry.key)));
    }

    public void testEntryMetadata()
    {
        BasicTSEntryConverter f = new BasicTSEntryConverter(BasicTSKeyConverter.defaultInstance());
        long creationTime = 0x1234567887654321L;
        ByteContainer bytes = f.createMetadata(creationTime,
                TSLastAccess.SIMPLE.asByte(), Integer.MAX_VALUE, Integer.MAX_VALUE / 3);
        byte[] raw = bytes.asBytes();
        StoredEntry<?> entry = f.entryFromStorable(null, null, raw, 0, raw.length);
        assertEquals(Long.toHexString(creationTime), Long.toHexString(entry.getCreationTime()));
        assertEquals(TSLastAccess.SIMPLE, entry.getLastAccessUpdateMethod());
        assertEquals(Integer.MAX_VALUE, entry.getMinTTLSinceAccessSecs());
        assertEquals(Integer.MAX_VALUE/3, entry.getMaxTTLSecs());

        creationTime = 0x1234567887654321L;
        bytes = f.createMetadata(creationTime,
                TSLastAccess.NONE.asByte(), Integer.MAX_VALUE/2, Integer.MAX_VALUE / 5);
        raw = bytes.asBytes();
        entry = f.entryFromStorable(null, null, raw, 0, raw.length);
        assertEquals(Long.toHexString(creationTime), Long.toHexString(entry.getCreationTime()));
        assertEquals(TSLastAccess.NONE, entry.getLastAccessUpdateMethod());
        assertEquals(Integer.MAX_VALUE/2, entry.getMinTTLSinceAccessSecs());
        assertEquals(Integer.MAX_VALUE/5, entry.getMaxTTLSecs());
    }
}
