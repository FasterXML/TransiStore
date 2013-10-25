package com.fasterxml.transistore.client;

import org.skife.config.TimeSpan;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.RequestPathBuilder;
import com.fasterxml.clustermate.client.StoreClientConfig;
import com.fasterxml.clustermate.client.call.PutCallParameters;

public class TSPutCallParameters extends PutCallParameters
{
    protected final static int NOT_DEFINED = -1;
	
    protected final int _maxTTLInSeconds;
	
    public TSPutCallParameters() {
        super((StoreClientConfig<?,?>) null);
        _maxTTLInSeconds = NOT_DEFINED;
    }

    protected TSPutCallParameters(TSPutCallParameters base, int maxTTLSecs)
    {
        super(base);
        _maxTTLInSeconds = maxTTLSecs;
    }

    protected TSPutCallParameters(TSPutCallParameters base, StoreClientConfig<?,?> config)
    {
        super(base, config);
        _maxTTLInSeconds = base._maxTTLInSeconds;
    }
    
    /*
    /**********************************************************************
    /* Mutant factories
    /**********************************************************************
     */

    public TSPutCallParameters withTTL(int ttlSecs) {
        return (ttlSecs == _maxTTLInSeconds) ? this : new TSPutCallParameters(this, ttlSecs);
    }

    public TSPutCallParameters withTTL(TimeSpan ttl) {
        int ttlSecs = (ttl == null) ? NOT_DEFINED : (int) (ttl.getMillis() / 1000L);
        return (ttlSecs == _maxTTLInSeconds) ? this : new TSPutCallParameters(this, ttlSecs);
    }

    @Override
    public TSPutCallParameters withClientConfig(StoreClientConfig<?,?> config) {
        return new TSPutCallParameters(this, config);
    }

    /*
    /**********************************************************************
    /* Implementation needed by base class
    /**********************************************************************
     */
	
	@SuppressWarnings("unchecked")
	@Override
	public <B extends RequestPathBuilder> B appendToPath(B pathBuilder,
			EntryKey contentId)
	{
		if (_maxTTLInSeconds > 0) {
			pathBuilder = (B) pathBuilder.addParameter(ClusterMateConstants.QUERY_PARAM_MAX_TTL, _maxTTLInSeconds);
		}
		return pathBuilder;
	}
}
