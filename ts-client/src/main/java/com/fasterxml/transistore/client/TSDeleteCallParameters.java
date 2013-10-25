package com.fasterxml.transistore.client;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.RequestPathBuilder;
import com.fasterxml.clustermate.client.StoreClientConfig;
import com.fasterxml.clustermate.client.call.DeleteCallParameters;

/**
 * A placeholder implementation; no functionality yet.
 */
public class TSDeleteCallParameters extends DeleteCallParameters
{
    public TSDeleteCallParameters() {
        super();
    }

    protected TSDeleteCallParameters(TSDeleteCallParameters base, StoreClientConfig<?,?> config)
    {
        super(base, config);
    }

    /*
    /**********************************************************************
    /* Mutant factories
    /**********************************************************************
     */

    @Override
    public TSDeleteCallParameters withClientConfig(StoreClientConfig<?,?> config) {
        return new TSDeleteCallParameters(this, config);
    }

    /*
    /**********************************************************************
    /* Implementation needed by base class
    /**********************************************************************
     */

    @Override
    public <B extends RequestPathBuilder> B appendToPath(B pathBuilder,
            EntryKey contentId)
    {
        // Nothing to do, yet!
        return pathBuilder;
    }
}
