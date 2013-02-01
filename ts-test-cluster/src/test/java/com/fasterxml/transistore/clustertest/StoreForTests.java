package com.fasterxml.transistore.clustertest;

import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.jetty.server.Server;

import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.config.ServerFactory;
import com.yammer.dropwizard.json.ObjectMapperFactory;
import com.yammer.dropwizard.validation.Validator;

import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.file.FileManager;
import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.clustertest.util.TimeMasterForClusterTesting;
import com.fasterxml.transistore.dw.BasicTSServiceConfigForDW;
import com.fasterxml.transistore.dw.BasicTSServiceOnDW;

/**
 * Specialized sub-class of {@link BasicTSServiceOnDW} used by unit tests.
 */
public class StoreForTests extends BasicTSServiceOnDW
{
    protected Server _jettyServer;

    /**
     * We keep type-safe reference to the <code>TimeMaster</code> 
     */
    protected final TimeMasterForClusterTesting _testTimer;

    /**
     * Flag to indicate whether full initialization of the service, including
     * background threads, should be done (true), or just minimal (false)
     */
    protected final boolean _fullInit;

    protected final AtomicBoolean _preStopped = new AtomicBoolean(false);

    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */

    protected StoreForTests(TimeMasterForClusterTesting tm, boolean fullInit) {
        super(tm, true);
        _testTimer = tm;
        _fullInit = fullInit;
    }

    /**
     * Method called by unit tests to set up a full HTTP service for testing
     * (either for stand-alone or cluster tests).
     * NOTE: <code>start()</code> method is not called; caller must do that.
     */
    public static StoreForTests createTestService(BasicTSServiceConfigForDW config,
            TimeMasterForClusterTesting timeMaster, boolean fullInit)
        throws Exception
    {
        StoreForTests service = new StoreForTests(timeMaster, fullInit);
        Bootstrap<BasicTSServiceConfigForDW> bootstrap = new Bootstrap<BasicTSServiceConfigForDW>(service);
        final Environment environment = new Environment("TestService", config,
                new ObjectMapperFactory(), new Validator());
        bootstrap.runWithBundles(config, environment);
        service.run(config, environment);
        final Server server = new ServerFactory(config.getHttpConfiguration(),
                "StoreForTests").buildServer(environment);
        service._jettyServer = server;
        return service;
    }

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */
    
    @Override
    public void start() throws Exception
    {
        _jettyServer.start();
        if (_fullInit) {
            super.start();
        }
    }

    @Override
    public void stop() throws Exception {
        // Let's ensure there are no threads blocked on virtual timer
        long wait = _testTimer.advanceTimeToWakeAll();
        // and apply tiny bit of delay iff time was advanced:
        if (wait > 0L) {
            Thread.sleep(20L);
        }
        prepareForStop();
        _jettyServer.stop();
    }

    public void waitForStopped() throws Exception {
        _testTimer.advanceTimeToWakeAll();
//        _jettyServer.join();
    }

    public void prepareForStop()
    {
        if (_fullInit) {
            if (!_preStopped.get()) {
                _preStopped.set(true);
                try {
                    super.stop();
                } catch (Exception e) {
                    System.err.printf("prepareForStop fail (%s): %s\n", e.getClass().getName(), e.getMessage());
                }
            }
        }
    }
    
    /*
    /**********************************************************************
    /* Accessors
    /**********************************************************************
     */

    public SharedServiceStuff getServiceStuff() {
        return _serviceStuff;
    }

    public StorableStore getEntryStore() {
        return _stores.getEntryStore();
    }
    
    public EntryKeyConverter<BasicTSKey> getKeyConverter() {
        return getServiceStuff().getKeyConverter();
    }

    @Override
    public TimeMasterForClusterTesting getTimeMaster() {
        return _testTimer;
    }

    public ClusterViewByServer getCluster() {
        return _cluster;
    }

    public FileManager getFileManager() {
        return _serviceStuff.getFileManager();
    }
}
