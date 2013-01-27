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
    
    public BasicTSClientConfigBuilder() {
        super(DEFAULT_KEY_CONVERTER, DEFAULT_BASE_PATH,
                DEFAULT_PATH_STRATEGY);
    }

    public BasicTSClientConfigBuilder(BasicTSClientConfig config) {
        super(config);
    }
    
    @Override
    public BasicTSClientConfig build() {
        return new BasicTSClientConfig(_keyConverter, _basePath, _jsonMapper,
                buildOperationConfig());
    }
}
