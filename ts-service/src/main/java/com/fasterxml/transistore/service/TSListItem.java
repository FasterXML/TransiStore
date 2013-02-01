package com.fasterxml.transistore.service;

import com.fasterxml.clustermate.api.msg.ListItem;
import com.fasterxml.storemate.store.Storable;

public class TSListItem extends ListItem
{
    // just for deserialization
    protected TSListItem() { }

    public TSListItem(Storable storable) {
        super(storable.getKey(), storable.getContentHash(), storable.getActualUncompressedLength());
    }
}
