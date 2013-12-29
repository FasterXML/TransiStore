package com.fasterxml.transistore.service.lastaccess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.storemate.store.lastaccess.LastAccessUpdateMethod;
import com.fasterxml.clustermate.service.lastaccess.LastAccessConverterBase;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.service.TSLastAccess;

public class BasicTSLastAccessConverter
    extends LastAccessConverterBase<BasicTSKey, StoredEntry<BasicTSKey>>
{
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    // Default impl ought to be ok here:
    /*
    @Override
    public EntryLastAccessed createLastAccessed(byte[] raw, int offset, int length)
    {
        if (length != 17) {
            throw new IllegalArgumentException("LastAccessed entry length must be 17 bytes, was: "+length);
        }
        long accessTime = ByteUtil.getLongBE(raw, offset);
        long expirationTime = ByteUtil.getLongBE(raw, offset+8);
        byte type = raw[16];
        return new EntryLastAccessed(accessTime, expirationTime, type);
    }
    */

    @Override
    public byte[] createLastAccessedKey(BasicTSKey key, LastAccessUpdateMethod method)
    {
        TSLastAccess acc = (TSLastAccess) method;
        if (acc != null) {
            switch (acc) {
            case NONE:
                return null;
            case SIMPLE: // whole key, for one-to-one match
                return key.asStorableKey().asBytes();
            }
        }
        LOG.warn("Missing or unrecognized 'accessMethod' value: {}", acc);
        return null;
    }
}
