package com.fasterxml.transistore.service.bdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;

import com.fasterxml.clustermate.service.LastAccessUpdateMethod;
import com.fasterxml.clustermate.service.bdb.BDBConverters;
import com.fasterxml.clustermate.service.bdb.LastAccessStore;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;
import com.fasterxml.transistore.basic.BasicTSKey;

public class BasicTSLastAccessStore
	extends LastAccessStore<BasicTSKey, StoredEntry<BasicTSKey>>
{
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    public BasicTSLastAccessStore(Environment env,
            StoredEntryConverter<BasicTSKey,StoredEntry<BasicTSKey>> conv)
    {
        super(env, conv);
    }

    @Override
    protected DatabaseEntry lastAccessKey(BasicTSKey key, LastAccessUpdateMethod acc)
    {
        if (acc != null) {
            switch (acc) {
            case NONE:
                return null;
            case GROUPED: // important: not just group id, but also client id
                return key.withPartitionId(BDBConverters.simpleConverter);
            case INDIVIDUAL: // whole key, including client id, group id length
                return key.asStorableKey().with(BDBConverters.simpleConverter);
            }
        }
        LOG.warn("Illegal 'accessMethod' value: {}", acc);
        return null;
    }
}
