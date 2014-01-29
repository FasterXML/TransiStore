package com.fasterxml.transistore.client.jdk;

import com.fasterxml.clustermate.client.jdk.BaseJdkHttpNetworkClient;
import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.client.BasicTSClientConfig;

public class JDKBasedClient
    extends BaseJdkHttpNetworkClient<BasicTSKey, BasicTSClientConfig>
{
    public JDKBasedClient(BasicTSClientConfig config) {
        super(config);
    }
}
