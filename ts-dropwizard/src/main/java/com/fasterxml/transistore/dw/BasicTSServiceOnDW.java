package com.fasterxml.transistore.dw;

import java.util.*;

import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.StoreOperationThrottler;
import com.fasterxml.storemate.store.file.FileManager;
import com.fasterxml.storemate.store.file.FileManagerConfig;
import com.fasterxml.storemate.store.state.NodeStateStore;

import com.fasterxml.clustermate.dw.DWBasedService;
import com.fasterxml.clustermate.dw.HealthCheckForCluster;
import com.fasterxml.clustermate.dw.HealthCheckForStore;
import com.fasterxml.clustermate.dw.RunMode;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.cleanup.CleanupTask;
import com.fasterxml.clustermate.service.cleanup.DiskUsageTracker;
import com.fasterxml.clustermate.service.cleanup.FileCleaner;
import com.fasterxml.clustermate.service.state.ActiveNodeState;
import com.fasterxml.clustermate.service.store.*;
import com.fasterxml.clustermate.servlet.CMServletFactory;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.basic.BasicTSListItem;
import com.fasterxml.transistore.dw.cmd.*;
import com.fasterxml.transistore.service.BasicTSOperationThrottler;
import com.fasterxml.transistore.service.SharedTSStuffImpl;
import com.fasterxml.transistore.service.cfg.BasicTSFileManager;
import com.fasterxml.transistore.service.cfg.BasicTSServiceConfig;
import com.fasterxml.transistore.service.cleanup.LocalEntryCleaner;
import com.fasterxml.transistore.service.store.BasicTSStoreHandler;
import com.fasterxml.transistore.service.store.BasicTSStores;
import com.fasterxml.transistore.servlet.BasicTSServletFactory;

/**
 * Main service class that sets up service configuration, bootstrapping things
 * and initializing life-cycle components and resources.
 */
public class BasicTSServiceOnDW
    extends DWBasedService<
        BasicTSKey, StoredEntry<BasicTSKey>,
        BasicTSServiceConfig, BasicTSServiceConfigForDW
    >
{
    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */

    protected BasicTSServiceOnDW(TimeMaster timings, RunMode mode) {
        super(timings, mode);
    }

    @Override
    public void initialize(Bootstrap<BasicTSServiceConfigForDW> bootstrap) {
        super.initialize(bootstrap);
//      addBundle(new com.yammer.dropwizard.views.ViewBundle());
        // Some basic commands that may prove useful
        bootstrap.addCommand(new CommandDumpBDB());
        bootstrap.addCommand(new CommandCleanBDB());
    }
    
    public static void main(String[] args) throws Exception
    {
        /* 04-Jun-2013, tatu: Alas, here a bit of nasty hackery is
         *   called for; problem being that we need to post-process
         *   configuration somewhat.
         */
        BasicTSServiceOnDW svc = new BasicTSServiceOnDW(TimeMaster.nonTestInstance(), RunMode.FULL);
        svc.run(args);
    }

    /*
    @Override
    public void run(BasicTSServiceConfigForDW configuration, Environment environment) throws java.io.IOException {
        super.run();
    }
    */

    /*
    protected void customRun(String[] arguments) throws Exception {
        final Bootstrap<BasicTSServiceConfigForDW> bootstrap = new Bootstrap<BasicTSServiceConfigForDW>(this);
        bootstrap.addCommand(new ServerCommand<BasicTSServiceConfigForDW>(this));
        initialize(bootstrap);
        final Cli cli = new Cli(this.getClass(), bootstrap);
        cli.run(arguments);
    }
    */
    
    /*
    /**********************************************************************
    /* Abstract method implementations from base class
    /**********************************************************************
     */

    @Override
    protected FileManager constructFileManager()
    {
        return new BasicTSFileManager(
                new FileManagerConfig(serviceConfig().storeConfig.dataRootForFiles),
                _timeMaster);
    }

    @Override
    protected SharedServiceStuff constructServiceStuff(BasicTSServiceConfig serviceConfig,
            TimeMaster timeMaster, StoredEntryConverter<BasicTSKey, StoredEntry<BasicTSKey>,?> entryConverter,
            FileManager files)
    {
        return new SharedTSStuffImpl(serviceConfig, timeMaster,
            entryConverter, files);
    }

    @Override
    protected StoresImpl<BasicTSKey, StoredEntry<BasicTSKey>> constructStores(StorableStore store,
            NodeStateStore<IpAndPort, ActiveNodeState> nodeStates)
    {
        StoredEntryConverter<BasicTSKey, StoredEntry<BasicTSKey>,BasicTSListItem> entryConv = _serviceStuff.getEntryConverter();
        return new BasicTSStores(serviceConfig(),
                _timeMaster, _serviceStuff.jsonMapper(), entryConv, store, nodeStates);
    }
    
    @Override
    protected StoreHandler<BasicTSKey, StoredEntry<BasicTSKey>,BasicTSListItem> constructStoreHandler() {
        // false -> no updating of last-accessed timestamps by default
        return new BasicTSStoreHandler(_serviceStuff, _stores, _cluster, false);
    }

    /*
    /**********************************************************************
    /* Overrides
    /**********************************************************************
     */

    @Override
    protected CMServletFactory constructServletFactory() {
        return new BasicTSServletFactory(_serviceStuff, _stores,
                _cluster, _clusterInfoHandler, _syncHandler, _storeHandler);
    }

    @Override
    protected void addHealthChecks(Environment environment)
    {
        ServiceConfig config = _serviceStuff.getServiceConfig();
        environment.healthChecks().register("store-check", new HealthCheckForStore(config, _stores));
        environment.healthChecks().register("cluster-check", new HealthCheckForCluster(config, _cluster));
    }

    @Override
    protected List<CleanupTask<?>> constructCleanupTasks()
    {
        ArrayList<CleanupTask<?>> tasks = new ArrayList<CleanupTask<?>>();
        // start with main entries
        tasks.add(new LocalEntryCleaner());
        // then remove orphan dirs
        tasks.add(new FileCleaner());
        // and finally disk space usage tracker
        tasks.add(new DiskUsageTracker());

        return tasks;
    }

    /**
     * Method is overridden to provide alternate throttler
     */
    @Override
    protected StoreOperationThrottler constructThrottler() {
        return new BasicTSOperationThrottler();
    }

    /*
    /**********************************************************************
    /* Extended API
    /**********************************************************************
     */

    public StoreHandler<BasicTSKey, StoredEntry<BasicTSKey>,?> getStoreHandler() {
        return _storeHandler;
    }
}
