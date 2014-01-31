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
    protected final int _maxHttpConnectionsPerHost;

    protected BasicTSClientConfig(EntryKeyConverter<BasicTSKey> keyConverter,
            String[] basePath, ObjectMapper jsonMapper,
            OperationConfig operConfig,
            int maxHttpConnections, int maxHttpConnectionsPerHost) {
        super(keyConverter, basePath, new BasicTSPaths(),
                jsonMapper, operConfig);
        _maxHttpConnections = maxHttpConnections;
        _maxHttpConnectionsPerHost = maxHttpConnectionsPerHost;
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

    /**
     * Helper method that can be used to truncate min/optimal/max success
     * counts, based on actual observed number of servers cluster has.
     */
    public BasicTSClientConfig verifyWithServerCount(int serverCount)
    {
        OperationConfig opConfig = getOperationConfig();
        if ((opConfig.getMinimalOksToSucceed() > serverCount)
                || (opConfig.getOptimalOks() > serverCount)
                || (opConfig.getMaxOks() > serverCount)) {
            BasicTSClientConfigBuilder builder = builder();
            if (opConfig.getMinimalOksToSucceed() > serverCount) {
                builder.setMinimalOksToSucceed(serverCount);
            }
            if (opConfig.getOptimalOks() > serverCount) {
                builder.setOptimalOks(serverCount);
            }
            if (opConfig.getMaxOks() > serverCount) {
                builder.setMaxOks(serverCount);
            }
            return builder.build();
        }
        return this;
    }

    public int getMaxHttpConnections() {
        return _maxHttpConnections;
    }

    public int getMaxHttpConnectionsPerHost() {
        return _maxHttpConnectionsPerHost;
    }
}
