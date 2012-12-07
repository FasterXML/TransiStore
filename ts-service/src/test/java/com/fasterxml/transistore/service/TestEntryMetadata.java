package com.fasterxml.transistore.service;

import junit.framework.TestCase;

import com.fasterxml.storemate.shared.ByteContainer;

import com.fasterxml.clustermate.service.LastAccessUpdateMethod;
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
                123L, 10, 100, LastAccessUpdateMethod.INDIVIDUAL);
        assertEquals("0x"+Integer.toHexString(entry.routingHashUsing(keyConverter)),
                "0x"+Integer.toHexString(keyConverter.routingHashFor(entry.key)));
    }

    public void testEntryMetadata()
    {
        BasicTSEntryConverter f = new BasicTSEntryConverter(BasicTSKeyConverter.defaultInstance());
        final long creationTime = 0x1234567887654321L;
        ByteContainer bytes = f.createMetadata(creationTime,
                LastAccessUpdateMethod.GROUPED, Integer.MAX_VALUE, Integer.MAX_VALUE / 3);
        byte[] raw = bytes.asBytes();
        StoredEntry<?> entry = f.entryFromStorable(null, null, raw, 0, raw.length);
        assertEquals(Long.toHexString(creationTime), Long.toHexString(entry.getCreationTime()));
        assertEquals(LastAccessUpdateMethod.GROUPED, entry.getLastAccessUpdateMethod());
        assertEquals(Integer.MAX_VALUE, entry.getMinTTLSinceAccessSecs());
        assertEquals(Integer.MAX_VALUE/3, entry.getMaxTTLSecs());
    }
}
