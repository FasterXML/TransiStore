package com.fasterxml.transistore.dw.util;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.KeyRange;
import com.fasterxml.clustermate.jaxrs.StoreResource;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.store.StoreHandler;
import com.fasterxml.clustermate.service.store.StoredEntry;

/**
 * Sub-class of {@link StoreResource}, used by unit tests.
 */
public class StoreResourceForTests<K extends EntryKey, E extends StoredEntry<K>>
    extends StoreResource<K, E>
{
    public StoreResourceForTests(ClusterViewByServer clusterView,
            StoreHandler<K,E,?> storeHandler, SharedServiceStuff stuff)
    {
        super(stuff, clusterView, storeHandler);
    }

    public KeyRange getKeyRange() {
        return _clusterView.getLocalState().getRangeActive();
    }

    // not visible from base impl so:
    public SharedServiceStuff getStuff() {
        return _stuff;
    }
}
