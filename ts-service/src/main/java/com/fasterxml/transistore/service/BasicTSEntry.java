package com.fasterxml.transistore.service;

import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.lastaccess.LastAccessUpdateMethod;

import com.fasterxml.transistore.basic.BasicTSKey;

public class BasicTSEntry extends StoredEntry<BasicTSKey>
{
    /*
    /**********************************************************************
    /* Entry contents
    /**********************************************************************
     */

    /**
     * Key used to refer to this entry
     */
    public final BasicTSKey key;

    /**
     * "Raw" serialized contents of the entry
     */
    public final Storable rawEntry;

    /**
     * Timestamp of actual creation of entry; used for TTL calculations.
     * Unlike insertion time (which may change), this will not be
     * changed once entry has been added.
     */
    public final long creationTime;

    /**
     * Maximum time-to-live, in seconds, since creation. After this time is
     * up, entry may be auto-expired, regardless of other settings.
     *<p>
     * This value may be sent by client; if not, a default value will be used.
     *<p>
     * Note that this setting has higher precedence that {@link #minTTLSinceAccessSecs},
     * meaning that it will effectively limit maximum time an entry can live.
     */
    public final int maxTTLSecs;

    /**
     * Minimum time-to-live, in seconds, since the entry was last accessed.
     *<p>
     * This value may be sent by client; if not, a default value will be used.
     *<p>
     * Note that this value has lower precedence than {@link #maxTTLSecs},
     * meaning that even if entry is regularly accessed, it can not
     * "live forever".
     */
    public final int minTTLSinceAccessSecs;

    public final LastAccessUpdateMethod lastAccessUpdateMethod;
    
    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */

    /**
     * Constructor used by unit tests, synchronization code
     */
    protected BasicTSEntry(BasicTSKey key, Storable rawEntry,
            long creationTime, int minTTLSecs, int maxTTLSecs,
            LastAccessUpdateMethod lastAccessUpdatedMethod)
    {
        this.key = key;
        this.rawEntry = rawEntry;
        this.creationTime = creationTime;
        this.minTTLSinceAccessSecs = minTTLSecs;
        this.maxTTLSecs = maxTTLSecs;
        this.lastAccessUpdateMethod = lastAccessUpdatedMethod;
    }

    /*
    /**********************************************************************
    /* Simple accessor implementations
    /**********************************************************************
     */

    @Override
    public BasicTSKey getKey() { return key; }

    @Override
    public Storable getRaw() { return rawEntry; }

    @Override
    public LastAccessUpdateMethod getLastAccessUpdateMethod() {
        return lastAccessUpdateMethod;
    }

    @Override
    public int getMinTTLSinceAccessSecs() {
        return minTTLSinceAccessSecs;
    }

    @Override
    public int getMaxTTLSecs() {
        return maxTTLSecs;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    /*
    /**********************************************************************
    /* Derived accessor implementations
    /**********************************************************************
     */

    @Override
    public boolean hasExceededMaxTTL(long currentTime) 
    {
        return (currentTime >= (creationTime + 1000L * maxTTLSecs));
    }

    @Override
    public boolean hasExceededMinTTL(long currentTime) 
    {
        return (currentTime >= (creationTime + 1000L * minTTLSinceAccessSecs));
    }
    
    @Override
    public boolean usesLastAccessTime()
    {
        LastAccessUpdateMethod method = lastAccessUpdateMethod;
        return (method != null) && !method.meansNoUpdate();
    }
    
    @Override
    public boolean hasExceededLastAccessTTL(long currentTime, long lastAccess)
    {
        // first: if no last-access info found, assume creation time is to be used
        // (or, if somehow last-access timestamp was corrupt?)
        long lastAccessMsecs = Math.max(creationTime, lastAccess);
        return (currentTime >= (lastAccessMsecs + minTTLSinceAccessSecs*1000));
    }

    @Override
    public boolean insertedBefore(long timestamp) {
        return rawEntry.getLastModified() < timestamp;
    }

    @Override
    public boolean createdBefore(long timestamp) {
        return creationTime < timestamp;
    }

    @Override
    public int routingHashUsing(EntryKeyConverter<BasicTSKey> hasher)
    {
        return hasher.routingHashFor(key);
    }

    /*
    /**********************************************************************
    /* Overridden standard methods
    /**********************************************************************
     */
    
    // Override for diagnostics, debugging:
    // Default impl is good actually, leave as is
    /*
    public String toString()
    {
        StringBuilder sb = new StringBuilder(60)
            .append("[key='").append(key);
        sb.append(", storageLength=").append(rawEntry.getStorageLength());
        sb.append(", compressed=").append(rawEntry.getCompression());
        sb.append(']');
        return sb.toString();
    }
    */
}
