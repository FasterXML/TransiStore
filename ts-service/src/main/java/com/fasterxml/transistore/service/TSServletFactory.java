package com.fasterxml.transistore.service;

import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cluster.ClusterInfoHandler;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServerUpdatable;
import com.fasterxml.clustermate.service.store.StoreHandler;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoresImpl;
import com.fasterxml.clustermate.service.sync.SyncHandler;
import com.fasterxml.clustermate.servlet.DefaultCMServletFactory;
import com.fasterxml.clustermate.servlet.ServletWithMetricsBase;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.basic.BasicTSListItem;
import com.fasterxml.transistore.service.cfg.BasicTSServiceConfig;

public class TSServletFactory extends DefaultCMServletFactory<
    BasicTSKey, StoredEntry<BasicTSKey>, BasicTSListItem,
    BasicTSServiceConfig
>
{
    public TSServletFactory(SharedServiceStuff stuff,
            StoresImpl<BasicTSKey, StoredEntry<BasicTSKey>> stores,
            ClusterViewByServerUpdatable cluster,
            ClusterInfoHandler clusterInfoHandler,
            SyncHandler<BasicTSKey,StoredEntry<BasicTSKey>> syncHandler,
            StoreHandler<BasicTSKey,StoredEntry<BasicTSKey>, BasicTSListItem> storeHandler)
    {
        super(stuff, stores, cluster, clusterInfoHandler, syncHandler, storeHandler);
    }

    @Override
    protected ServletWithMetricsBase constructStoreEntryServlet() {
        return new BasicTSStoreEntryServlet(_serviceStuff, _cluster, _storeHandler);
    }
}
