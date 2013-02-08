package com.fasterxml.transistore.basic;

import com.fasterxml.clustermate.api.msg.ListItem;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.storemate.shared.StorableKey;

// Let's actually fix serialization order, to help client stability
@JsonPropertyOrder({
    "key", "hash", "length", // from base class
    "created", "maxTTL"
})
public class BasicTSListItem extends ListItem
{
    /**
     * Creation time in usual JDK milliseconds since epoch.
     */
    public long created;

    /**
     * Maximum time-to-live in seconds (since {@link #created})
     */
    public int maxTTL;
    
    // just for deserialization
    protected BasicTSListItem() { }

    public BasicTSListItem(StorableKey rawKey, int hash, long length,
            long creationTime, int maxTTLSecs)
    {
        super(rawKey, hash, length);
        created = creationTime;
        maxTTL = maxTTLSecs;
    }
}
