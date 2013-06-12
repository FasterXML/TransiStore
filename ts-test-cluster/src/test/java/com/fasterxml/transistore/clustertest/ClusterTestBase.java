package com.fasterxml.transistore.clustertest;

import java.io.*;

import com.fasterxml.storemate.backend.bdbje.BDBJEConfig;
import com.fasterxml.storemate.store.backend.StoreBackendConfig;

/**
 * Shared base class for unit tests; contains shared utility methods.
 */
public abstract class ClusterTestBase extends GenericClusterTestBase
{
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
