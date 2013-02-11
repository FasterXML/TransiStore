package com.fasterxml.transistore.service;

import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.clustermate.api.msg.ListItem;
import com.fasterxml.clustermate.service.LastAccessUpdateMethod;
import com.fasterxml.clustermate.service.bdb.BDBConverters;
import com.fasterxml.clustermate.service.store.EntryLastAccessed;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;
import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.util.WithBytesCallback;
import com.fasterxml.storemate.store.Storable;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.basic.BasicTSKeyConverter;
import com.fasterxml.transistore.basic.BasicTSListItem;

public class BasicTSEntryConverter
    extends StoredEntryConverter<BasicTSKey, StoredEntry<BasicTSKey>, BasicTSListItem>
{
    public final static byte V_METADATA_VERSION_1 = 0x11;
    
    public final static int OFFSET_VERSION = 0;
    public final static int OFFSET_LAST_ACCESS = 1;

    public final static int OFFSET_CREATE_TIME = 4;
    public final static int OFFSET_MIN_TTL = 12;
    public final static int OFFSET_MAX_TTL = 16;

    public final static int METADATA_LENGTH = 20;

    protected final EntryKeyConverter<BasicTSKey> _keyConverter;

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    public BasicTSEntryConverter() {
        this(BasicTSKeyConverter.defaultInstance());
    }
    
    public BasicTSEntryConverter(EntryKeyConverter<BasicTSKey> keyConverter) {
        _keyConverter = keyConverter;
    }

    /*
    /**********************************************************************
    /* Pass-through methods for key construction
    /**********************************************************************
     */

    @Override
    public EntryKeyConverter<BasicTSKey> keyConverter() {
        return _keyConverter;
    }
    
    /*
    /**********************************************************************
    /* Conversions for metadata section
    /**********************************************************************
     */
    
    /**
     * Method called to construct "custom metadata" section to be
     * used for constructing a new <code>Storable</code> instance.
     */
    @Override
    public ByteContainer createMetadata(long creationTime,
            byte lastAccessUpdateMethod, int minTTLSecs, int maxTTLSecs)
    {
        byte[] buffer = new byte[METADATA_LENGTH];
        buffer[OFFSET_VERSION] = V_METADATA_VERSION_1;
        buffer[OFFSET_LAST_ACCESS] = lastAccessUpdateMethod;
        _putLongBE(buffer, OFFSET_CREATE_TIME, creationTime);
        _putIntBE(buffer, OFFSET_MIN_TTL, minTTLSecs);
        _putIntBE(buffer, OFFSET_MAX_TTL, maxTTLSecs);
     
        return ByteContainer.simple(buffer, 0, METADATA_LENGTH);
    }

    /*
    /**********************************************************************
    /* Actual Entry conversions
    /**********************************************************************
     */
    
    @Override
    public final BasicTSEntry entryFromStorable(final Storable raw) {
        return entryFromStorable(_key(raw.getKey()), raw);
    }

    @Override
    public final BasicTSEntry entryFromStorable(final BasicTSKey key, final Storable raw)
    {
        return raw.withMetadata(new WithBytesCallback<BasicTSEntry>() {
            @Override
            public BasicTSEntry withBytes(byte[] buffer, int offset, int length) {
                return entryFromStorable(key, raw, buffer, offset, length);
            }
        });
    }

    @Override
    public BasicTSEntry entryFromStorable(BasicTSKey key, Storable raw,
            byte[] buffer, int offset, int length)
    {
        int version = _extractVersion(key, buffer, offset, length);
        if (version != V_METADATA_VERSION_1) {
            _badData(key, "version 0x"+Integer.toHexString(version));
        }

        LastAccessUpdateMethod acc = _extractLastAccessUpdatedMethod(key, buffer, offset, length);
        final long creationTime = _extractCreationTime(buffer, offset, length);
        final int minTTLSecs = _extractMinTTLSecs(buffer, offset, length);
        final int maxTTLSecs = _extractMaxTTLSecs(buffer, offset, length);

        return new BasicTSEntry(key, raw, creationTime, minTTLSecs, maxTTLSecs, acc);
    }

    @Override
    public ListItem minimalListItemFromStorable(Storable raw) {
        return defaultMinimalListItemFromStorable(raw);
    }
    
    @Override
    public BasicTSListItem fullListItemFromStorable(Storable raw) {
        BasicTSEntry entry = entryFromStorable(raw);
        return new BasicTSListItem(raw.getKey(), raw.getContentHash(), raw.getActualUncompressedLength(),
                entry.creationTime, entry.maxTTLSecs);
    }
    
    /*
    /**********************************************************************
    /* Other conversions
    /**********************************************************************
     */

    @Override
    public EntryLastAccessed createLastAccessed(StoredEntry<BasicTSKey> entry, long accessTime)
    {
        return new EntryLastAccessed(accessTime, entry.getCreationTime(), 
                 entry.getLastAccessUpdateMethod().asByte());
    }

    @Override
    public EntryLastAccessed createLastAccessed(byte[] raw) {
        return createLastAccessed(raw, 0, raw.length);
    }

    @Override
    public EntryLastAccessed createLastAccessed(byte[] raw, int offset, int length)
    {
        if (length != 17) {
            throw new IllegalArgumentException("LastAccessed entry length must be 16 bytes, was: "+length);
        }
        long accessTime = BDBConverters.getLongBE(raw, offset);
        long creationTime = BDBConverters.getLongBE(raw, offset+8);
        byte type = raw[16];
        return new EntryLastAccessed(accessTime, creationTime, type);
    }
    
    /*
    /**********************************************************************
    /* Internal methods, data extraction
    /**********************************************************************
     */

    protected int _extractVersion(BasicTSKey key, byte[] buffer, int offset, int length) {
        return buffer[offset+OFFSET_VERSION];
    }

    protected long _extractCreationTime(byte[] buffer, int offset, int length) {
        return _getLongBE(buffer, offset+OFFSET_CREATE_TIME);
    }

    protected int _extractMinTTLSecs(byte[] buffer, int offset, int length) {
        return _getIntBE(buffer, offset+OFFSET_MIN_TTL);
    }

    protected int _extractMaxTTLSecs(byte[] buffer, int offset, int length) {
        return _getIntBE(buffer, offset+OFFSET_MAX_TTL);
    }
    
    protected LastAccessUpdateMethod _extractLastAccessUpdatedMethod(BasicTSKey key, byte[] buffer, int offset, int length)
    {
        int accCode = buffer[offset+OFFSET_LAST_ACCESS];
        LastAccessUpdateMethod acc = TSLastAccess.valueOf(accCode);
        if (acc == null) {
            _badData(key, "invalid last-access-update-method 0x"+Integer.toHexString(accCode));
        }
        return acc;
    }

    private final static void _putLongBE(byte[] buffer, int offset, long value)
    {
        _putIntBE(buffer, offset, (int) (value >> 32));
        _putIntBE(buffer, offset+4, (int) value);
    }
    
    private final static void _putIntBE(byte[] buffer, int offset, int value)
    {
        buffer[offset] = (byte) (value >> 24);
        buffer[++offset] = (byte) (value >> 16);
        buffer[++offset] = (byte) (value >> 8);
        buffer[++offset] = (byte) value;
    }

    private final static long _getLongBE(byte[] buffer, int offset)
    {
        long l1 = _getIntBE(buffer, offset);
        long l2 = _getIntBE(buffer, offset+4);
        return (l1 << 32) | ((l2 << 32) >>> 32);
    }
    
    private final static int _getIntBE(byte[] buffer, int offset)
    {
        return (buffer[offset] << 24)
             | ((buffer[++offset] & 0xFF) << 16)
             | ((buffer[++offset] & 0xFF) << 8)
             | (buffer[++offset] & 0xFF)
             ;
    }

    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */
    
    protected void _badData(final BasicTSKey key, String msg) {
        throw new IllegalArgumentException("Bad BasicTSKey metadata (key "+key+"): "+msg);
    }
    
    protected BasicTSKey _key(StorableKey rawKey) {
        return _keyConverter.rawToEntryKey(rawKey);
    }
}
