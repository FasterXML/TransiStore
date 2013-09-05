package com.fasterxml.transistore.client;

import org.skife.config.TimeSpan;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.RequestPathBuilder;
import com.fasterxml.clustermate.client.call.PutCallParameters;

public class TSPutCallParameters extends PutCallParameters
{
	protected final static int NOT_DEFINED = -1;
	
	protected final int _maxTTLInSeconds;

	public TSPutCallParameters() {
		this(NOT_DEFINED);
	}

	protected TSPutCallParameters(int maxTTLSecs)
	{
		_maxTTLInSeconds = maxTTLSecs;
	}

    /*
    /**********************************************************************
    /* Mutant factories
    /**********************************************************************
     */

	public TSPutCallParameters withTTL(int ttlSecs) {
		return (ttlSecs == _maxTTLInSeconds) ? this : new TSPutCallParameters(ttlSecs);
	}
	
	public TSPutCallParameters withTTL(TimeSpan ttl) {
		int ttlSecs = (ttl == null) ? NOT_DEFINED : (int) (ttl.getMillis() / 1000L);
		return (ttlSecs == _maxTTLInSeconds) ? this : new TSPutCallParameters(ttlSecs);
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
