package com.fasterxml.transistore.service;

import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.StoreConfig;
import com.fasterxml.storemate.store.file.FileManager;

import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.service.cfg.BasicTSServiceConfig;


public class SharedTSStuffImpl
    extends SharedTSStuff
{
    private final BasicTSServiceConfig _serviceConfig;

    private final StoredEntryConverter<BasicTSKey, StoredEntry<BasicTSKey>> _entryConverter;

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    public SharedTSStuffImpl(BasicTSServiceConfig config, TimeMaster timeMaster,
            StoredEntryConverter<BasicTSKey, StoredEntry<BasicTSKey>> entryConverter,
            FileManager fileManager)
    {
        super(timeMaster, fileManager, config.getServicePathStrategy());
        _serviceConfig = config;
        _entryConverter = entryConverter;
    }
    
    /*
    /**********************************************************************
    /* Basic config access
    /**********************************************************************
     */

    @SuppressWarnings("unchecked")
    @Override
    public <C extends ServiceConfig> C getServiceConfig() {
        return (C) _serviceConfig;
    }

    @Override
    public StoreConfig getStoreConfig() {
     return _serviceConfig.storeConfig;
    }

    @Override
    public StoredEntryConverter<BasicTSKey, StoredEntry<BasicTSKey>> getEntryConverter() {
        return _entryConverter;
    }

    @Override
    public EntryKeyConverter<BasicTSKey> getKeyConverter() {
        return _entryConverter.keyConverter();
    }
}
