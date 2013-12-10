package com.fasterxml.transistore.service.bdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;

import com.fasterxml.clustermate.service.bdb.BDBConverters;
import com.fasterxml.clustermate.service.bdb.BDBLastAccessStore;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;
import com.fasterxml.storemate.store.lastaccess.LastAccessConfig;
import com.fasterxml.storemate.store.lastaccess.LastAccessUpdateMethod;
import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.service.TSLastAccess;

public class BasicTSLastAccessStore
	extends BDBLastAccessStore<BasicTSKey, StoredEntry<BasicTSKey>>
{
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    public BasicTSLastAccessStore(Environment env,
            StoredEntryConverter<BasicTSKey,StoredEntry<BasicTSKey>,?> conv,
            LastAccessConfig config)
    {
        super(env, conv, config);
    }
    
    @Override
    protected DatabaseEntry lastAccessKey(BasicTSKey key, LastAccessUpdateMethod acc0)
    {
        TSLastAccess acc = (TSLastAccess) acc0;
        if (acc != null) {
            switch (acc) {
            case NONE:
                return null;
            case SIMPLE: // whole key, for one-to-one match
                return key.asStorableKey().with(BDBConverters.simpleConverter);
            }
        }
        LOG.warn("Illegal 'accessMethod' value: {}", acc);
        return null;
    }
}
