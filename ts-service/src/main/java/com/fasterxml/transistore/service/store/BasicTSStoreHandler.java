package com.fasterxml.transistore.service.store;

import com.fasterxml.clustermate.service.*;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.store.StoreHandler;
import com.fasterxml.clustermate.service.store.StoredEntry;

import com.fasterxml.transistore.basic.BasicTSKey;

public class BasicTSStoreHandler extends StoreHandler<BasicTSKey, StoredEntry<BasicTSKey>>
{
    public BasicTSStoreHandler(SharedServiceStuff stuff,
            Stores<BasicTSKey, StoredEntry<BasicTSKey>> stores,
            ClusterViewByServer cluster)
    {
        super(stuff, stores, cluster);
    }

    /*
    /**********************************************************************
    /* Extracting last-accessed/updated info from key
    /**********************************************************************
     */

    @Override
    protected LastAccessUpdateMethod _findLastAccessUpdateMethod(BasicTSKey key)
    {
        return key.hasPartitionId() ? LastAccessUpdateMethod.GROUPED
                : LastAccessUpdateMethod.INDIVIDUAL;
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
