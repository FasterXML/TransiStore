package com.fasterxml.transistore.service.cfg;

import com.fasterxml.clustermate.api.RequestPathStrategy;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;
import com.fasterxml.storemate.store.backend.StoreBackendConfig;
import com.fasterxml.transistore.basic.BasicTSKeyConverter;
import com.fasterxml.transistore.basic.BasicTSPaths;
import com.fasterxml.transistore.service.BasicTSEntryConverter;


/**
 * Configuration Object that Vagabond itself uses; either read from
 * a JSON or YAML file, or programmatically constructed.
 * Typically contained in a wrapper Object.
 */
public class BasicTSServiceConfig
    extends ServiceConfig
{
    protected RequestPathStrategy _requestPathStrategy;

    public BasicTSServiceConfig() {
        this(new BasicTSPaths());
    }

    public BasicTSServiceConfig(RequestPathStrategy paths) {
        super();
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
        return new BasicTSEntryConverter(BasicTSKeyConverter.defaultInstance());
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
