package com.fasterxml.transistore.clustertest;

import java.io.*;

import com.fasterxml.storemate.backend.bdbje.BDBJEConfig;
import com.fasterxml.storemate.store.backend.StoreBackendConfig;

/**
 * Shared base class for unit tests; contains shared utility methods.
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

    /**
     * This compile-time flag is here to remind that tests use (or not)
     * BDB-JE transctions. This should make no difference under normal test
     * conditions, since we don't do anything to cause problems. But
     * at higher level we may experience other issues with shutdown.
     */
    protected final static boolean USE_TRANSACTIONS = false;
    
    /*
    /**********************************************************************
    /* Configuration setting helpers
    /**********************************************************************
     */

    @Override
    protected StoreBackendConfig createBackendConfig(File dataDir)
    {
        BDBJEConfig config = new BDBJEConfig(dataDir);
        // 03-Oct-2013, tatu: Should we verify that BDB-JE transactions may
        //   be used?
        config.useTransactions = USE_TRANSACTIONS;
        return config;
    }
}
