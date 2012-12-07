package com.fasterxml.transistore.client;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.clustermate.client.impl.StoreClientConfig;
import com.fasterxml.clustermate.client.operation.OperationConfig;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.basic.BasicTSPaths;

public class BasicTSClientConfig
    extends StoreClientConfig<BasicTSKey, BasicTSClientConfig>
{
    protected BasicTSClientConfig(EntryKeyConverter<BasicTSKey> keyConverter,
            String[] basePath, ObjectMapper jsonMapper,
            OperationConfig operConfig) {
        super(keyConverter, basePath, new BasicTSPaths(),
                jsonMapper, operConfig);
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
}
