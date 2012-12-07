package com.fasterxml.transistore.basic;

import com.fasterxml.storemate.shared.EntryKey;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.util.WithBytesAsUTF8String;
import com.fasterxml.storemate.shared.util.WithBytesCallback;

/**
 * Value class that contains identifier used for accessing
 * piece of content: basically contains optional partition id
 * and String.
 */
public class BasicTSKey
    extends EntryKey
{
    /*
    /**********************************************************************
    /* Main configuration
    /**********************************************************************
     */
    
    /**
     * Raw representation used for the underlying store
     */
    private final StorableKey _rawKey;

    /**
     * Length of group id, in bytes (not necessarily characters).
     */
    private final int _partitionIdLength;
    
    /**
     * Full path, including Partition id as prefix.
     */
    private transient String _externalPath;
    
    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */

    protected BasicTSKey(StorableKey raw, int partitionIdLength)
    {
        _rawKey = raw;
        _partitionIdLength = partitionIdLength;
    }
    
    /*
    /**********************************************************************
    /* Accessors, converters
    /**********************************************************************
     */

    @Override
    public StorableKey asStorableKey() {
        return _rawKey;
    }

    @Override
    public byte[] asBytes() {
        return _rawKey.asBytes();
    }
    
    /*
    /**********************************************************************
    /* Extended API
    /**********************************************************************
     */

    /**
     * Accessor for getting full path, where partition id is the prefix,
     * and path follows it right after.
     */
    public String getExternalPath()
    {
        String str = _externalPath;
        if (str == null) {
            final int offset = BasicTSKeyConverter.DEFAULT_KEY_HEADER_LENGTH;
            final int length = _rawKey.length() - offset;
            _externalPath = str = _rawKey.withRange(WithBytesAsUTF8String.instance, offset, length);
        }
        return str;
    }
    
    public String getPartitionId()
    {
        if (_partitionIdLength == 0) {
            return null;
        }
        return getExternalPath().substring(0, _partitionIdLength);
    }

    public byte[] getPartitionIdAsBytes()
    {
        if (_partitionIdLength == 0) {
            return null;
        }
        return _rawKey.rangeAsBytes(BasicTSKeyConverter.DEFAULT_KEY_HEADER_LENGTH, _partitionIdLength);
    }

    /**
     * Callback-based accessor for accessing part of key formed when path itself
     * is dropped, and only client id and group id are included.
     * Note that method can only be called when there is a group id; otherwise
     * a {@link IllegalStateException} will be thrown.
     */
    public <T> T withPartitionPrefix(WithBytesCallback<T> cb)
    {
        if (_partitionIdLength <= 0) {
            throw new IllegalStateException("Key does not have a partition id, can not call this method");
        }
        int len = BasicTSKeyConverter.DEFAULT_KEY_HEADER_LENGTH + _partitionIdLength;
        return _rawKey.withRange(cb, 0, len);
    }
    
    public boolean hasPartitionId() {
        return (_partitionIdLength > 0);
    }

    public int getPartitionIdLength() {
        return _partitionIdLength;
    }
    
    /*
    /**********************************************************************
    /* Overridden std methods
    /**********************************************************************
     */
    
    @Override public boolean equals(Object o)
    {
        if (o == this) return true;
        if (o == null) return false;
        if (o.getClass() != getClass()) return false;
        BasicTSKey other = (BasicTSKey) o;
        return _rawKey.equals(other._rawKey);
    }

    @Override public int hashCode() {
        return _rawKey.hashCode();
    }

    @Override public String toString() {
        if (_partitionIdLength == 0) {
            return "0:"+getExternalPath();
        }
        return String.valueOf(_partitionIdLength)+":"+getExternalPath();
    }
}
