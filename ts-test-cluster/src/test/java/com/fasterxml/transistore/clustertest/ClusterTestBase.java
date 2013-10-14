package com.fasterxml.transistore.clustertest;

import java.io.*;

import com.fasterxml.clustermate.client.StoreClientBootstrapper;
import com.fasterxml.storemate.backend.bdbje.BDBJEConfig;
import com.fasterxml.storemate.backend.leveldb.LevelDBConfig;
import com.fasterxml.storemate.store.backend.StoreBackendConfig;
import com.fasterxml.transistore.client.BasicTSClientConfig;

/**
 * Shared base class for unit tests, regardless of backend DB
 * or http client used.
 */
public abstract class ClusterTestBase extends GenericClusterTestBase
{
    protected static int PORT_BASE_SINGLE = 7100;
    protected static int PORT_BASE_DUAL = 7300;
    protected static int PORT_BASE_QUAD = 7600;

    protected static int PORT_DELTA_SIMPLE = 10;
    protected static int PORT_DELTA_SYNC = 30;
    protected static int PORT_DELTA_CONFLICT = 50;
    protected static int PORT_DELTA_LIST = 60;
    protected static int PORT_DELTA_RANGE = 70;
    protected static int PORT_DELTA_EXPIRATION = 80;
    
    /*
    /**********************************************************************
    /* Abstract methods for actual tests to implement
    /**********************************************************************
     */

    @Override
    protected abstract StoreBackendConfig createBackendConfig(File dataDir);

    /*
    /**********************************************************************
    /* Helper methods for subclasses; backend DB
    /**********************************************************************
     */

    protected StoreBackendConfig bdbBackendConfig(File dataDir)
    {
        BDBJEConfig config = new BDBJEConfig(dataDir);
        return config;
    }

    protected StoreBackendConfig levelDBBackendConfig(File dataDir)
    {
        LevelDBConfig config = new LevelDBConfig(dataDir);
        return config;
    }
    
    /*
    /**********************************************************************
    /* Helper methods for subclasses; HTTP Client
    /**********************************************************************
     */

    protected StoreClientBootstrapper<?,?,?,?> bootstrapperWithAHC(BasicTSClientConfig clientConfig)
    {
        return new com.fasterxml.transistore.client.ahc.AHCBasedClientBootstrapper(clientConfig);
    }

    protected StoreClientBootstrapper<?,?,?,?> bootstrapperWithJDK(BasicTSClientConfig clientConfig)
    {
        return new com.fasterxml.transistore.client.jdk.JDKBasedClientBootstrapper(clientConfig);
    }
}
