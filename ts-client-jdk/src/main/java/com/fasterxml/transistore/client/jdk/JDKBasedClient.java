package com.fasterxml.transistore.client.jdk;

import com.fasterxml.clustermate.api.PathType;
import com.fasterxml.clustermate.client.jdk.BaseJdkHttpNetworkClient;
import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.client.BasicTSClientConfig;

public class JDKBasedClient
    extends BaseJdkHttpNetworkClient<BasicTSKey,PathType,BasicTSClientConfig>
{
    public JDKBasedClient(BasicTSClientConfig config) {
        super(config, PathType.STORE_ENTRY, PathType.STORE_LIST);
    }
}
