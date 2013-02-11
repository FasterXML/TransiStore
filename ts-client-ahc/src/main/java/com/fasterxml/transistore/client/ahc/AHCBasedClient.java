package com.fasterxml.transistore.client.ahc;

import com.fasterxml.clustermate.client.ahc.BaseAHCBasedNetworkClient;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.client.BasicTSClientConfig;

public class AHCBasedClient
    extends BaseAHCBasedNetworkClient<
    BasicTSKey, BasicTSClientConfig>
{
    public AHCBasedClient(BasicTSClientConfig config)
    {
        super(config);
    }
}
