package com.fasterxml.transistore.service.store;

import com.fasterxml.clustermate.service.*;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.store.StoreHandler;
import com.fasterxml.clustermate.service.store.StoredEntry;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.service.TSLastAccess;
import com.fasterxml.transistore.service.TSListItem;

public class BasicTSStoreHandler extends StoreHandler<BasicTSKey,
    StoredEntry<BasicTSKey>, TSListItem>
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
    
    /*
    /**********************************************************************
    /* Extracting last-accessed/updated info from key
    /**********************************************************************
     */

    @Override
    protected LastAccessUpdateMethod _findLastAccessUpdateMethod(ServiceRequest request, BasicTSKey key)
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
        _updateLastAccessed(entry.getKey(), entry, accessTime);
    }

    @Override
    protected void updateLastAccessedForHead(ServiceRequest request, ServiceResponse response,
            StoredEntry<BasicTSKey> entry, long accessTime)
    {
        _updateLastAccessed(entry.getKey(), entry, accessTime);
    }
    
    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */
    
    private void _updateLastAccessed(BasicTSKey key, StoredEntry<BasicTSKey> entry, long accessTime)
    {
        _stores.getLastAccessStore().updateLastAccess(entry, accessTime);
    }
}
