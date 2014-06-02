package com.fasterxml.transistore.servlet;

import java.util.*;

import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.cluster.ClusterInfoHandler;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServerUpdatable;
import com.fasterxml.clustermate.service.metrics.AllOperationMetrics;
import com.fasterxml.clustermate.service.metrics.BackgroundMetricsAccessor;
import com.fasterxml.clustermate.service.store.StoreHandler;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoresImpl;
import com.fasterxml.clustermate.service.sync.SyncHandler;
import com.fasterxml.clustermate.servlet.*;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.basic.BasicTSPath;

/**
 * Implementation of {@link CMServletFactory} used by standard TransiStore.
 * Defines server-side handling of request routing.
 * Could define
 */
public class BasicTSServletFactory
    extends CMServletFactory
{
    /*
    /**********************************************************************
    /* Basic config
    /**********************************************************************
     */

    protected final ServiceConfig _config;
    
    protected final SharedServiceStuff _serviceStuff;

    /*
    /**********************************************************************
    /* External state
    /**********************************************************************
     */

    protected final StoresImpl<BasicTSKey,StoredEntry<BasicTSKey>> _stores;

    /**
     * And we better hang on to cluster view as well
     */
    protected final ClusterViewByServerUpdatable _cluster;

    /*
    /**********************************************************************
    /* Service handlers
    /**********************************************************************
     */

    protected final ClusterInfoHandler _clusterInfoHandler;

    protected final SyncHandler<BasicTSKey,StoredEntry<BasicTSKey>> _syncHandler;
    
    protected final StoreHandler<BasicTSKey,StoredEntry<BasicTSKey>,?> _storeHandler;

    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */
    
    public BasicTSServletFactory(SharedServiceStuff stuff,
            StoresImpl<BasicTSKey,StoredEntry<BasicTSKey>> stores, ClusterViewByServerUpdatable cluster,
            ClusterInfoHandler clusterInfoHandler,
            SyncHandler<BasicTSKey,StoredEntry<BasicTSKey>> syncHandler,
            StoreHandler<BasicTSKey,StoredEntry<BasicTSKey>,?> storeHandler)
    {
        _serviceStuff = stuff;
        _config = stuff.getServiceConfig();

        _stores = stores;
        _cluster = cluster;

        _clusterInfoHandler = clusterInfoHandler;
        _syncHandler = syncHandler;
        _storeHandler = storeHandler;
    }

    /*
    /**********************************************************************
    /* Public API impl
    /**********************************************************************
     */
    
    @Override
    public ServletBase contructDispatcherServlet()
    {
        EnumMap<BasicTSPath, ServletBase> servlets = new EnumMap<BasicTSPath, ServletBase>(BasicTSPath.class);

        ServletBase statusServlet = constructNodeStatusServlet();

        _add(servlets, BasicTSPath.NODE_STATUS, statusServlet);

        _add(servlets, BasicTSPath.SYNC_LIST, constructSyncListServlet());
        _add(servlets, BasicTSPath.SYNC_PULL, constructSyncPullServlet());

        _add(servlets, BasicTSPath.STORE_ENTRY, constructStoreEntryServlet());
        _add(servlets, BasicTSPath.STORE_ENTRY_INFO, constructStoreEntryInfoServlet());
        _add(servlets, BasicTSPath.STORE_ENTRIES, constructStoreListServlet());

        // remote sync/pull does differ from local ones, to some degree
        _add(servlets, BasicTSPath.REMOTE_SYNC_LIST, constructRemoteSyncListServlet());
        _add(servlets, BasicTSPath.REMOTE_SYNC_PULL, constructRemoteSyncPullServlet());
        
        List<AllOperationMetrics.Provider> metrics = new ArrayList<AllOperationMetrics.Provider>();
        for (ServletBase servlet : servlets.values()) {
            if (servlet instanceof AllOperationMetrics.Provider) {
                metrics.add((AllOperationMetrics.Provider) servlet);
            }
        }

        // "remote status" doesn't really differ from local status; register it now
        // since stats shared between remote/local
        _add(servlets, BasicTSPath.REMOTE_STATUS, statusServlet);
        
        final BackgroundMetricsAccessor metricAcc = constructMetricsAccessor(metrics);
        servlets.put(BasicTSPath.NODE_METRICS, constructNodeMetricsServlet(metricAcc));
        return new ServiceDispatchServlet<BasicTSKey,StoredEntry<BasicTSKey>,BasicTSPath>(_cluster, null, _serviceStuff, servlets);
    }

    /*
    /**********************************************************************
    /* Factory methods: metrics
    /**********************************************************************
     */

    protected BackgroundMetricsAccessor constructMetricsAccessor(List<AllOperationMetrics.Provider> metrics) {
        AllOperationMetrics.Provider[] providers = metrics.toArray(new AllOperationMetrics.Provider[metrics.size()]);
        return new BackgroundMetricsAccessor(_serviceStuff, _stores, providers);
    }

    /*
    /**********************************************************************
    /* Factory methods: servlets, standard
    /**********************************************************************
     */

    protected ServletBase constructStoreEntryInfoServlet() {
        return new StoreEntryInfoServlet<BasicTSKey,StoredEntry<BasicTSKey>>(_serviceStuff, _cluster, _storeHandler);
    }
    
    protected ServletBase constructNodeStatusServlet() {
        return new NodeStatusServlet(_serviceStuff, _clusterInfoHandler);
    }

    protected ServletBase constructNodeMetricsServlet(BackgroundMetricsAccessor accessor) {
        return new NodeMetricsServlet(_serviceStuff, accessor);
    }

    protected ServletBase constructSyncListServlet() {
        return new SyncListServlet<BasicTSKey,StoredEntry<BasicTSKey>>(_serviceStuff, _cluster, _syncHandler);
    }

    protected ServletBase constructSyncPullServlet() {
        return new SyncPullServlet<BasicTSKey,StoredEntry<BasicTSKey>>(_serviceStuff, _cluster, _syncHandler);
    }

    protected ServletBase constructStoreListServlet() {
        return new StoreListServlet<BasicTSKey,StoredEntry<BasicTSKey>>(_serviceStuff, _cluster, _storeHandler);
    }


    protected ServletBase constructRemoteSyncListServlet() {
        return new RemoteSyncListServlet<BasicTSKey,StoredEntry<BasicTSKey>>(_serviceStuff, _cluster, _syncHandler);
    }

    protected ServletBase constructRemoteSyncPullServlet() {
        return new RemoteSyncPullServlet<BasicTSKey,StoredEntry<BasicTSKey>>(_serviceStuff, _cluster, _syncHandler);
    }

    /*
    /**********************************************************************
    /* Factory methods: servlets, custom
    /**********************************************************************
     */

    protected ServletWithMetricsBase constructStoreEntryServlet() {
        return new BasicTSStoreEntryServlet(_serviceStuff, _cluster, _storeHandler);
    }
    
    /*
    /**********************************************************************
    /* Helper methods
    /**********************************************************************
     */
    
    protected <T extends Enum<T>> void _add(EnumMap<T, ServletBase> servlets,
            T path, ServletBase servlet)
    {
        if (servlet != null) {
            servlets.put(path, servlet);
        }
    }
}
