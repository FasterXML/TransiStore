package com.fasterxml.transistore.client;

import com.fasterxml.clustermate.client.StoreClientConfigBuilder;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.basic.BasicTSKeyConverter;
import com.fasterxml.transistore.basic.BasicTSPaths;

public class BasicTSClientConfigBuilder
    extends StoreClientConfigBuilder<BasicTSKey, BasicTSClientConfig, BasicTSClientConfigBuilder>
{
    protected final static BasicTSKeyConverter DEFAULT_KEY_CONVERTER = BasicTSKeyConverter.defaultInstance();

    /**
     * Default path assumes that server-side stuff is deployed under a single
     * logical directory.
     */
    protected final static String[] DEFAULT_BASE_PATH = new String[] { "ts" };

    protected final static BasicTSPaths DEFAULT_PATH_STRATEGY = new BasicTSPaths();

    protected final static int DEFAULT_MAX_HTTP_CONNECTIONS = 50;
    
    /**
     * Default settings for some HttpClients (like AHC) are quite modest; so
     * let's allow overrides
     */
    protected int _maxHttpConnections = DEFAULT_MAX_HTTP_CONNECTIONS;
    
    public BasicTSClientConfigBuilder() {
        super(DEFAULT_KEY_CONVERTER, DEFAULT_BASE_PATH,
                DEFAULT_PATH_STRATEGY);
    }

    public BasicTSClientConfigBuilder(BasicTSClientConfig config) {
        super(config);
        _maxHttpConnections = config.getMaxHttpConnections();
    }
    
    @Override
    public BasicTSClientConfig build() {
        return new BasicTSClientConfig(_keyConverter, _basePath, _jsonMapper,
                buildOperationConfig(),
                _maxHttpConnections);
    }

    public BasicTSClientConfigBuilder setMaxHttpConnections(int max) {
        _maxHttpConnections = max;
        return this;
    }
}
