package com.fasterxml.transistore.service.cfg;

import com.fasterxml.storemate.store.backend.StoreBackendConfig;

import com.fasterxml.clustermate.api.RequestPathStrategy;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;

import com.fasterxml.transistore.basic.BasicTSPaths;
import com.fasterxml.transistore.service.BasicTSEntryConverter;

/**
 * Configuration Object that service uses; either read from
 * a JSON or YAML file, or programmatically constructed.
 * Typically contained in a wrapper Object.
 */
public class BasicTSServiceConfig
    extends ServiceConfig
{
    /**
     * By default, we'll use "/ts/" as the service root.
     */
    protected final static String[] DEFAULT_SERVICE_ROOT = new String[] {
        "ts"
    };
    
    protected RequestPathStrategy _requestPathStrategy;

    protected StoredEntryConverter<?,?> _entryConverter;

    public BasicTSServiceConfig() {
        this(new BasicTSEntryConverter());
    }
    
    public BasicTSServiceConfig(StoredEntryConverter<?,?> entryConverter) {
        this(entryConverter, new BasicTSPaths());
    }

    public BasicTSServiceConfig(StoredEntryConverter<?,?> entryConverter,
            RequestPathStrategy paths) {
        super(DEFAULT_SERVICE_ROOT);
        _entryConverter = entryConverter;
        _requestPathStrategy = paths;
    }

    /*
    /**********************************************************************
    /* Abstract method impl
    /**********************************************************************
     */
    
    @Override
    public RequestPathStrategy getServicePathStrategy() {
        return _requestPathStrategy;
    }
    
    @Override
    public StoredEntryConverter<?,?> getEntryConverter() {
        return _entryConverter;
    }
    
    /*
    /**********************************************************************
    /* Additional mutators
    /**********************************************************************
     */

    public BasicTSServiceConfig overrideStoreBackendConfig(StoreBackendConfig cfg) {
        _storeBackendConfigOverride = cfg;
        return this;
    }
}
