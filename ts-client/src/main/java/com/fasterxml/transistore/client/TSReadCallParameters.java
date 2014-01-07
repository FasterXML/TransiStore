package com.fasterxml.transistore.client;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.RequestPathBuilder;
import com.fasterxml.clustermate.client.StoreClientConfig;
import com.fasterxml.clustermate.client.call.ReadCallParameters;

/**
 * A placeholder implementation; no functionality yet.
 */
public class TSReadCallParameters extends ReadCallParameters
{
    public TSReadCallParameters() {
        super();
    }

    protected TSReadCallParameters(TSReadCallParameters base, StoreClientConfig<?,?> config)
    {
        super(base, config);
    }

    /*
    /**********************************************************************
    /* Mutant factories
    /**********************************************************************
     */

    @Override
    public TSReadCallParameters withClientConfig(StoreClientConfig<?,?> config) {
        return new TSReadCallParameters(this, config);
    }

    /*
    /**********************************************************************
    /* Implementation needed by base class
    /**********************************************************************
     */

    @Override
    public <B extends RequestPathBuilder<B>> B appendToPath(B pathBuilder,
            EntryKey contentId)
    {
        // Nothing to do, yet!
        return pathBuilder;
    }

}
