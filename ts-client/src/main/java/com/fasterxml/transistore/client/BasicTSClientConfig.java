package com.fasterxml.transistore.client;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.clustermate.client.StoreClientConfig;
import com.fasterxml.clustermate.client.operation.OperationConfig;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.basic.BasicTSPaths;

public class BasicTSClientConfig
    extends StoreClientConfig<BasicTSKey, BasicTSClientConfig>
{
    protected final int _maxHttpConnections;
    
    protected BasicTSClientConfig(EntryKeyConverter<BasicTSKey> keyConverter,
            String[] basePath, ObjectMapper jsonMapper,
            OperationConfig operConfig, int maxHttpConnections) {
        super(keyConverter, basePath, new BasicTSPaths(),
                jsonMapper, operConfig);
        _maxHttpConnections = maxHttpConnections;
    }

    @SuppressWarnings("unchecked")
    @Override
    public BasicTSClientConfigBuilder builder() {
        return new BasicTSClientConfigBuilder(this);
    }
    
    /**
     * Helper method for building instance that uses defaults and is to
     * be used on behalf of <code>clientId</code>.
     */
    public static BasicTSClientConfig defaultConfig() {
        return new BasicTSClientConfigBuilder().build();
    }

    public int getMaxHttpConnections() {
        return _maxHttpConnections;
    }
}
