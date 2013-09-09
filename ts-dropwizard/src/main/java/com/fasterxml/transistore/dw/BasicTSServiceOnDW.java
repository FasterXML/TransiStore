package com.fasterxml.transistore.dw;

import java.util.*;

import com.yammer.dropwizard.cli.Cli;
import com.yammer.dropwizard.cli.ServerCommand;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.yammer.metrics.core.HealthCheck;

import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.StoreOperationThrottler;
import com.fasterxml.storemate.store.file.FileManager;
import com.fasterxml.storemate.store.file.FileManagerConfig;

import com.fasterxml.clustermate.dw.DWBasedService;
import com.fasterxml.clustermate.dw.RunMode;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.Stores;
import com.fasterxml.clustermate.service.cleanup.CleanupTask;
import com.fasterxml.clustermate.service.cleanup.FileCleaner;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.store.*;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.basic.BasicTSListItem;
import com.fasterxml.transistore.dw.cmd.*;
import com.fasterxml.transistore.service.BasicTSOperationThrottler;
import com.fasterxml.transistore.service.SharedTSStuffImpl;
import com.fasterxml.transistore.service.cfg.BasicTSFileManager;
import com.fasterxml.transistore.service.cfg.BasicTSServiceConfig;
import com.fasterxml.transistore.service.cleanup.LastAccessCleaner;
import com.fasterxml.transistore.service.cleanup.LocalEntryCleaner;
import com.fasterxml.transistore.service.store.BasicTSStoreHandler;
import com.fasterxml.transistore.service.store.BasicTSStores;

/**
 * Main service class that sets up service configuration, bootstrapping things
 * and initializing life-cycle components and resources.
 */
public class BasicTSServiceOnDW
    extends DWBasedService<BasicTSKey, StoredEntry<BasicTSKey>, BasicTSListItem,
        BasicTSServiceConfig, BasicTSServiceConfigForDW>
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
        // and FreeMarker for more dynamic pages?
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
        new BasicTSServiceOnDW(TimeMaster.nonTestInstance(), RunMode.FULL).customRun(args);
    }

    /* Goddamit; original run() is final. Bah, humbug. Need to copy, modify...
     */
    protected void customRun(String[] arguments) throws Exception {
        final Bootstrap<BasicTSServiceConfigForDW> bootstrap = new Bootstrap<BasicTSServiceConfigForDW>(this);
        bootstrap.addCommand(new ServerCommand<BasicTSServiceConfigForDW>(this));
        initialize(bootstrap);
        final Cli cli = new Cli(this.getClass(), bootstrap);
        cli.run(arguments);
    }
    
    /*
    /**********************************************************************
    /* Abstract method implementations from base class
    /**********************************************************************
     */

    @SuppressWarnings("unchecked")
    @Override
    protected StoredEntryConverter<BasicTSKey, StoredEntry<BasicTSKey>,BasicTSListItem> constructEntryConverter(BasicTSServiceConfig config,
            Environment environment) {
        return (StoredEntryConverter<BasicTSKey, StoredEntry<BasicTSKey>,BasicTSListItem>) config.getEntryConverter();
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
            TimeMaster timeMaster, StoredEntryConverter<BasicTSKey, StoredEntry<BasicTSKey>,BasicTSListItem> entryConverter,
            FileManager files)
    {
        return new SharedTSStuffImpl(serviceConfig, timeMaster,
            entryConverter, files);
    }

    @Override
    protected StoresImpl<BasicTSKey, StoredEntry<BasicTSKey>> constructStores(SharedServiceStuff stuff,
            BasicTSServiceConfig serviceConfig, StorableStore store)
    {
        StoredEntryConverter<BasicTSKey, StoredEntry<BasicTSKey>,BasicTSListItem> entryConv = stuff.getEntryConverter();
        return new BasicTSStores(serviceConfig,
                _timeMaster, stuff.jsonMapper(), entryConv, store);
    }
    
    @Override
    protected StoreHandler<BasicTSKey, StoredEntry<BasicTSKey>,BasicTSListItem> constructStoreHandler(SharedServiceStuff serviceStuff,
            Stores<BasicTSKey, StoredEntry<BasicTSKey>> stores,
            ClusterViewByServer cluster) {
        // false -> no updating of last-accessed timestamps by default
        return new BasicTSStoreHandler(serviceStuff, _stores, cluster, false);
    }

    @Override
    protected BasicTSStoreEntryServlet constructStoreEntryServlet(SharedServiceStuff stuff,
            ClusterViewByServer cluster,
            StoreHandler<BasicTSKey, StoredEntry<BasicTSKey>,BasicTSListItem> storeHandler)
    {
        return new BasicTSStoreEntryServlet(stuff, _cluster, storeHandler);
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
        /* TODO: Once DropWizard 0.7 out, convert these, require new Metrics
         * 
         */
        /*
        ServiceConfig config = stuff.getServiceConfig();
        environment.addHealthCheck(new HealthCheckForStore(config, _stores));
        environment.addHealthCheck(new HealthCheckForCluster(config, _cluster));
*/
        HealthCheck hc = new HealthCheck("bogusCheck-pre-DW-0.7") {
            @Override
            protected Result check() throws Exception {
                return Result.healthy("Fine and Dandy!");
            }
        };
        environment.addHealthCheck(hc);
    }

    @Override
    protected List<CleanupTask<?>> constructCleanupTasks()
    {
        ArrayList<CleanupTask<?>> tasks = new ArrayList<CleanupTask<?>>();
        // start with main entries
        tasks.add(new LocalEntryCleaner());
        // then remove orphan dirs
        tasks.add(new FileCleaner());
        // and finally last-accessed entries
        tasks.add(new LastAccessCleaner());

        return tasks;
    }

    /**
     * Method is overridden to provide alternate throttler
     */
    @Override
    protected StoreOperationThrottler _constructThrottler(SharedServiceStuff stuff)
    {
        return new BasicTSOperationThrottler();
    }

    /*
    /**********************************************************************
    /* Extended API
    /**********************************************************************
     */

    public StoreHandler<BasicTSKey, StoredEntry<BasicTSKey>,BasicTSListItem> getStoreHandler() {
        return _storeHandler;
    }
}
