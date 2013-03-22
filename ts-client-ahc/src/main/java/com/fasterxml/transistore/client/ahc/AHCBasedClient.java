package com.fasterxml.transistore.client.ahc;

import com.fasterxml.clustermate.client.ahc.BaseAHCBasedNetworkClient;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.client.BasicTSClientConfig;
import com.ning.http.client.AsyncHttpClientConfig;

public class AHCBasedClient
    extends BaseAHCBasedNetworkClient<BasicTSKey, BasicTSClientConfig>
{
    public AHCBasedClient(BasicTSClientConfig config)
    {
        super(config);
    }

    @Override
    protected AsyncHttpClientConfig buildAHCConfig(BasicTSClientConfig config,
            AsyncHttpClientConfig.Builder ahcConfigBuilder)
    {
        return ahcConfigBuilder
                .setMaximumConnectionsTotal(config.getMaxHttpConnections())
                .setMaximumConnectionsPerHost(config.getMaxHttpConnectionsPerHost())
                .build();
    }
}
