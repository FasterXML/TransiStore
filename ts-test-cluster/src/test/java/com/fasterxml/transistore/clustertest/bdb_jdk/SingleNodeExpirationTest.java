package com.fasterxml.transistore.clustertest.bdb_jdk;

import java.io.File;

import com.fasterxml.storemate.store.backend.StoreBackendConfig;

import com.fasterxml.clustermate.client.StoreClientBootstrapper;

import com.fasterxml.transistore.client.BasicTSClientConfig;
import com.fasterxml.transistore.clustertest.base.single.SingleNodeExpirationTestBase;

public class SingleNodeExpirationTest extends SingleNodeExpirationTestBase
{
    @Override
    protected StoreBackendConfig createBackendConfig(File dataDir) {
        return bdbBackendConfig(dataDir);
    }

    @Override
    protected StoreClientBootstrapper<?, ?, ?, ?> createClientBootstrapper(BasicTSClientConfig clientConfig) {
        return bootstrapperWithJDK(clientConfig);
    }
}
