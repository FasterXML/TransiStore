package com.fasterxml.transistore.service.store;

import java.io.File;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.sleepycat.je.Environment;

import com.fasterxml.storemate.backend.bdbje.BDBLastAccessStoreImpl;
import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.lastaccess.LastAccessConfig;
import com.fasterxml.storemate.store.lastaccess.LastAccessStore;
import com.fasterxml.storemate.store.lastaccess.LastAccessUpdateMethod;
import com.fasterxml.storemate.store.state.NodeStateStore;

import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.state.ActiveNodeState;
import com.fasterxml.clustermate.service.store.*;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.service.lastaccess.BasicTSLastAccessConverter;

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
    protected boolean _openLocalStores(boolean log, boolean allowCreate, boolean writeAccess) {
        return true;
    }

    @Override
    protected void _prepareToCloseLocalStores() { }

    @Override
    protected void _closeLocalStores() { }
}
