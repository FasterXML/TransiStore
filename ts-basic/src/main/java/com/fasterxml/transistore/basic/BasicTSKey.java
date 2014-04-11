package com.fasterxml.transistore.basic;

import com.fasterxml.clustermate.api.EntryKey;
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
    implements Comparable<BasicTSKey>
{
    /**
     * Let's model external key presentation as an URI of sort.
     */
    public final static String KEY_PREFIX = "tstore://";

    /**
     * And use meow char for separating optional partition from path
     */
    public final static char KEY_SEPARATOR = '@';
    
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
     * Accessor for getting path part of key, not including partition id.
     */
    public String getPath()
    {
        String full = getPartitionAndPath();
        if (_partitionIdLength == 0) {
            return full;
        }
        return full.substring(_partitionIdLength);
    }

    /**
     * Accessor for getting concatenation of partition id (if any) and path.
     */
    public String getPartitionAndPath()
    {
        String str = _externalPath;
        if (str == null) {
            final int offset = BasicTSKeyConverter.DEFAULT_KEY_HEADER_LENGTH;
            final int length = _rawKey.length() - offset;
            _externalPath = str = _rawKey.withRange(WithBytesAsUTF8String.instance, offset, length);
        }
        return str;
    }

    /**
     * Accessor for getting partition id part of key, if any; if no partition id,
     * returns null.
     */
    public String getPartitionId()
    {
        if (_partitionIdLength == 0) {
            return null;
        }
        return getPartitionAndPath().substring(0, _partitionIdLength);
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

    @Override public String toString()
    {
        String path = getPartitionAndPath();
        final int pathLen = path.length();
        StringBuilder sb = new StringBuilder(10 + pathLen);
        sb.append(KEY_PREFIX);
        int partLen = getPartitionIdLength();
        // !!! TODO: implement escaping properly
        if (partLen == 0) {
            sb.append(KEY_SEPARATOR);
            sb.append(path);
        } else {
            sb.append(path);
            sb.insert(KEY_PREFIX.length() + partLen, KEY_SEPARATOR);
        }
        return sb.toString();
    }

    @Override
    public int compareTo(BasicTSKey o) {
        return _rawKey.compareTo(o._rawKey);
    }
}
