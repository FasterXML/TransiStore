package com.fasterxml.transistore.service.store;

import com.fasterxml.clustermate.service.*;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.store.DeferredDeleter;
import com.fasterxml.clustermate.service.store.StoreHandler;
import com.fasterxml.clustermate.service.store.StoredEntry;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.basic.BasicTSListItem;
import com.fasterxml.transistore.service.TSLastAccess;

public class BasicTSStoreHandler extends StoreHandler<BasicTSKey,
    StoredEntry<BasicTSKey>, BasicTSListItem>
{
    /**
     * Flag mostly used by tests to force updates of last-accessed time stamps.
     */
    protected boolean _defaultUpdateLastAccess = false;
    
    public BasicTSStoreHandler(SharedServiceStuff stuff,
            Stores<BasicTSKey, StoredEntry<BasicTSKey>> stores,
            ClusterViewByServer cluster,
            boolean defaultUpdateLastAccess)
    {
        super(stuff, stores, cluster);
        _defaultUpdateLastAccess = defaultUpdateLastAccess;
    }

    @Override
    protected DeferredDeleter constructDeleter(SharedServiceStuff stuff,
            Stores<BasicTSKey,?> stores)
    {
        // 28-May-2013, tatu: For now, better keep DELETEs synchronous for tests.
        if (stuff.isRunningTests()) {
            return DeferredDeleter.nonDeferring(stores.getEntryStore());
        }
        return new DeferredDeleter(stores.getEntryStore(), stuff.getServiceConfig().deletes);
    }

    /*
    /**********************************************************************
    /* Extracting last-accessed/updated info from key
    /**********************************************************************
     */

    @Override
    protected TSLastAccess _findLastAccessUpdateMethod(ServiceRequest request,
            BasicTSKey key)
    {
        /* 31-Jan-2012, tatu: For now let's only enable last-access time tracking
         *   for tests, since there's no good way to pass that with request, and it simply adds
         *   overhead without benefits (for now)
         */
        if (_defaultUpdateLastAccess) {
            return TSLastAccess.SIMPLE;
        }
        return TSLastAccess.NONE;
    }

    /*
    /**********************************************************************
    /* Updates to last-accessed/updated info
    /**********************************************************************
     */
        
    @Override
    protected void updateLastAccessedForGet(ServiceRequest request, ServiceResponse response,
            StoredEntry<BasicTSKey> entry, long accessTime)
    {
        _updateLastAccessed(request, entry, accessTime);
    }

    @Override
    protected void updateLastAccessedForHead(ServiceRequest request, ServiceResponse response,
            StoredEntry<BasicTSKey> entry, long accessTime)
    {
        _updateLastAccessed(request, entry, accessTime);
    }

    @Override
    protected void updateLastAccessedForDelete(ServiceRequest request, ServiceResponse response,
            BasicTSKey key, long deletionTime)
    {
        TSLastAccess acc = _findLastAccessUpdateMethod(request, key);
        // can only do straight delete with one-to-one mappings
        if (acc == TSLastAccess.SIMPLE) {
            _stores.getLastAccessStore().removeLastAccess(key, acc, deletionTime);
        }
    }
    
    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */
    
    private void _updateLastAccessed(ServiceRequest request, 
            StoredEntry<BasicTSKey> entry, long accessTime)
    {
        BasicTSKey key = entry.getKey();
        TSLastAccess acc = _findLastAccessUpdateMethod(request, key);
        if (acc != null && !acc.meansNoUpdate()) {
            _stores.getLastAccessStore().updateLastAccess(entry, accessTime);
        }
    }
}
