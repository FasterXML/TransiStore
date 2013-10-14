package com.fasterxml.transistore.clustertest.leveldb_ahc;

import java.io.File;

import com.fasterxml.storemate.store.backend.StoreBackendConfig;
import com.fasterxml.clustermate.client.StoreClientBootstrapper;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.transistore.client.BasicTSClientConfig;
import com.fasterxml.transistore.clustertest.base.single.SingleNodeExpirationTestBase;

public class SingleNodeExpirationTest extends SingleNodeExpirationTestBase
{
    @Override
    protected StoreBackendConfig createBackendConfig(ServiceConfig serviceConfig,
            File dataDir) {
        return levelDBBackendConfig(serviceConfig, dataDir);
    }

    @Override
    protected StoreClientBootstrapper<?, ?, ?, ?> createClientBootstrapper(BasicTSClientConfig clientConfig) {
        return bootstrapperWithAHC(clientConfig);
    }
}
