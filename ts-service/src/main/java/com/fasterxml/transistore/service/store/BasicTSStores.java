package com.fasterxml.transistore.service.store;

import java.io.File;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.state.NodeStateStore;

import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.state.ActiveNodeState;
import com.fasterxml.clustermate.service.store.*;

import com.fasterxml.transistore.basic.BasicTSKey;

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
            File dbEnvRoot)
    {
        super(config, timeMaster, jsonMapper, entryConverter, entryStore, nodeStates, dbEnvRoot);
    }

    @Override
    protected boolean _openLocalStores(boolean log, boolean allowCreate, boolean writeAccess) {
        return true;
    }

    @Override
    protected void _prepareToCloseLocalStores() { }

    @Override
    protected void _closeLocalStores() { }
}
