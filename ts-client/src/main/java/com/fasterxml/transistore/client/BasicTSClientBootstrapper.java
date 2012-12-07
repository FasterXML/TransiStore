package com.fasterxml.transistore.client;

import com.fasterxml.clustermate.api.ClusterStatusAccessor;
import com.fasterxml.clustermate.client.NetworkClient;
import com.fasterxml.clustermate.client.cluster.ClusterViewByClientImpl;
import com.fasterxml.clustermate.client.impl.StoreClientBootstrapper;

import com.fasterxml.transistore.basic.BasicTSKey;

public class BasicTSClientBootstrapper
    extends StoreClientBootstrapper<BasicTSKey, BasicTSClientConfig,
        BasicTSClient, BasicTSClientBootstrapper>
{
    public BasicTSClientBootstrapper(BasicTSClientConfig config, NetworkClient<BasicTSKey> hc)
    {
        super(config, hc);
    }

    @Override
    protected BasicTSClient _buildClient(BasicTSClientConfig config, ClusterStatusAccessor accessor,
            ClusterViewByClientImpl<BasicTSKey> clusterView, NetworkClient<BasicTSKey> client)
    {
        return new BasicTSClient(_config, _accessor, clusterView, _httpClient);
    }
}
