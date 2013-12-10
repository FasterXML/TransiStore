package com.fasterxml.transistore.service.store;

import java.io.File;

import com.sleepycat.je.Environment;

import com.fasterxml.clustermate.service.LastAccessStore;
import com.fasterxml.clustermate.service.cfg.LastAccessConfig;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.state.ActiveNodeState;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;
import com.fasterxml.clustermate.service.store.StoresImpl;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.state.NodeStateStore;
import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.service.bdb.BasicTSLastAccessStore;

public class BasicTSStores extends StoresImpl<BasicTSKey, StoredEntry<BasicTSKey>>
{
    public BasicTSStores(ServiceConfig config, TimeMaster timeMaster, ObjectMapper jsonMapper,
            StoredEntryConverter<BasicTSKey, StoredEntry<BasicTSKey>,?> entryFactory,
            StorableStore entryStore, NodeStateStore<IpAndPort, ActiveNodeState> nodeStates) {
        this(config, timeMaster, jsonMapper,
                entryFactory, entryStore, nodeStates, null);
    }

    public BasicTSStores(ServiceConfig config, TimeMaster timeMaster, ObjectMapper jsonMapper,
            StoredEntryConverter<BasicTSKey, StoredEntry<BasicTSKey>,?> entryConverter,
            StorableStore entryStore, NodeStateStore<IpAndPort, ActiveNodeState> nodeStates,
            File bdbEnvRoot)
    {
        super(config, timeMaster, jsonMapper, entryConverter, entryStore, nodeStates, bdbEnvRoot);
    }

    @Override
    protected LastAccessStore<BasicTSKey, StoredEntry<BasicTSKey>> buildAccessStore(Environment env,
            LastAccessConfig config) {
        return new BasicTSLastAccessStore(env, _entryConverter, config);
    }
}
