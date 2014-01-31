package com.fasterxml.transistore.client;

import com.fasterxml.clustermate.api.ClusterStatusAccessor;
import com.fasterxml.clustermate.client.ClusterViewByClient;
import com.fasterxml.clustermate.client.NetworkClient;
import com.fasterxml.clustermate.client.StoreClient;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.basic.BasicTSListItem;

public class BasicTSClient
    extends StoreClient<BasicTSKey, BasicTSClientConfig, BasicTSListItem>
{
    public BasicTSClient(BasicTSClientConfig config,
            ClusterStatusAccessor statusAccessor, ClusterViewByClient<BasicTSKey> clusterView,
            NetworkClient<BasicTSKey> httpClientImpl)
    {
        super(config, BasicTSListItem.class, statusAccessor, clusterView, httpClientImpl);
    }

    protected BasicTSClient(BasicTSClient base, BasicTSClientConfig newConfig) {
        super(base, newConfig);
    }
    
    public BasicTSClient withConfig(BasicTSClientConfig config)
    {
        if (config == _config) { // no change? just return this instance
            return this;
        }
        return new BasicTSClient(this, config);
    }
}
