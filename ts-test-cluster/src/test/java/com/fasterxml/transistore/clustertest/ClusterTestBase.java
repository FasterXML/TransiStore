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

    /*
    /**********************************************************************
    /* Configuration setting helpers
    /**********************************************************************
     */

    @Override
    protected StoreBackendConfig createBackendConfig(File dataDir) {
        return new BDBJEConfig(dataDir);
    }
}
