package com.fasterxml.transistore.dw;

import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.file.FileManager;
import com.fasterxml.storemate.store.file.FileManagerConfig;

import com.fasterxml.clustermate.dw.DWBasedService;
import com.fasterxml.clustermate.dw.HealthCheckForBDB;
import com.fasterxml.clustermate.dw.HealthCheckForCluster;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.Stores;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.servlet.StoreEntryServlet;
import com.fasterxml.clustermate.service.store.StoreHandler;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;
import com.fasterxml.clustermate.service.store.StoresImpl;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.dw.cmd.*;
import com.fasterxml.transistore.service.SharedTSStuffImpl;
import com.fasterxml.transistore.service.cfg.BasicTSFileManager;
import com.fasterxml.transistore.service.cfg.BasicTSServiceConfig;
import com.fasterxml.transistore.service.store.BasicTSStoreHandler;
import com.fasterxml.transistore.service.store.BasicTSStores;

import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;

/**
 * Main service class that sets up service configuration, bootstrapping things
 * and initializing life-cycle components and resources.
 */
public class BasicTSServiceOnDW
    extends DWBasedService<BasicTSKey, StoredEntry<BasicTSKey>,
        BasicTSServiceConfig, BasicTSServiceConfigForDW>
{
    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */

    protected BasicTSServiceOnDW(TimeMaster timings) {
        this(timings, false);
    }

    protected BasicTSServiceOnDW(TimeMaster timings, boolean testMode) {
        super(timings, testMode);
        
    }

    @Override
    public void initialize(Bootstrap<BasicTSServiceConfigForDW> bootstrap) {
        super.initialize(bootstrap);
        // and FreeMarker for more dynamic pages?
//      addBundle(new com.yammer.dropwizard.views.ViewBundle());
        // Some basic commands that may prove useful
        bootstrap.addCommand(new CommandDumpBDB());
        bootstrap.addCommand(new CommandCleanBDB());
    }
    
    public static void main(String[] args) throws Exception
    {
        new BasicTSServiceOnDW(TimeMaster.nonTestInstance()).run(args);
    }
    
    /*
    /**********************************************************************
    /* Abstract method implementations from base class
    /**********************************************************************
     */

    @SuppressWarnings("unchecked")
    @Override
    protected StoredEntryConverter<BasicTSKey, StoredEntry<BasicTSKey>> constructEntryConverter(BasicTSServiceConfig config,
            Environment environment) {
        return (StoredEntryConverter<BasicTSKey, StoredEntry<BasicTSKey>>) config.getEntryConverter();
    }

    @Override
    protected FileManager constructFileManager(BasicTSServiceConfig serviceConfig)
    {
        return new BasicTSFileManager(
                new FileManagerConfig(serviceConfig.storeConfig.dataRootForFiles),
                _timeMaster);
    }

    @Override
    protected SharedServiceStuff constructServiceStuff(BasicTSServiceConfig serviceConfig,
            TimeMaster timeMaster, StoredEntryConverter<BasicTSKey, StoredEntry<BasicTSKey>> entryConverter,
            FileManager files)
    {
        return new SharedTSStuffImpl(serviceConfig, timeMaster,
            entryConverter, files);
    }

    @Override
    protected StoresImpl<BasicTSKey, StoredEntry<BasicTSKey>> constructStores(SharedServiceStuff stuff,
            BasicTSServiceConfig serviceConfig, StorableStore store)
    {
        StoredEntryConverter<BasicTSKey, StoredEntry<BasicTSKey>> entryConv = stuff.getEntryConverter();
        return new BasicTSStores(serviceConfig,
                _timeMaster, stuff.jsonMapper(), entryConv, store);
    }
    
    @Override
    protected StoreHandler<BasicTSKey, StoredEntry<BasicTSKey>> constructStoreHandler(SharedServiceStuff serviceStuff,
            Stores<BasicTSKey, StoredEntry<BasicTSKey>> stores) {
        return new BasicTSStoreHandler(serviceStuff, _stores);
    }

    @Override
    protected StoreEntryServlet<BasicTSKey, StoredEntry<BasicTSKey>> constructStoreEntryServlet(SharedServiceStuff stuff,
            ClusterViewByServer cluster, StoreHandler<BasicTSKey, StoredEntry<BasicTSKey>> storeHandler)
    {
        return new StoreEntryServlet<BasicTSKey, StoredEntry<BasicTSKey>>(stuff, _cluster, storeHandler);
    }

    /*
    /**********************************************************************
    /* Overrides
    /**********************************************************************
     */

    @Override
    protected void addHealthChecks(SharedServiceStuff stuff,
            Environment environment)
    {
        ServiceConfig config = stuff.getServiceConfig();
        environment.addHealthCheck(new HealthCheckForBDB(config, _stores));
        environment.addHealthCheck(new HealthCheckForCluster(config, _cluster));
    }
}
