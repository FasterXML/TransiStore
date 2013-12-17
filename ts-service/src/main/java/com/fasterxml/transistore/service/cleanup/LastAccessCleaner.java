package com.fasterxml.transistore.service.cleanup;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.*;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.StoreException;
import com.fasterxml.storemate.store.backend.IterationAction;
import com.fasterxml.storemate.store.lastaccess.EntryLastAccessed;
import com.fasterxml.storemate.store.lastaccess.LastAccessStore;
import com.fasterxml.storemate.store.lastaccess.LastAccessStore.LastAccessIterationCallback;
import com.fasterxml.storemate.store.lastaccess.LastAccessUpdateMethod;
import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.service.BasicTSEntry;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.Stores;
import com.fasterxml.clustermate.service.cleanup.CleanupTask;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;

/**
 * Helper class used to keep track of clean up progress
 * for local BDB cleanup.
 */
public class LastAccessCleaner
    extends CleanupTask<LastAccessCleanupStats>
{
    private final static Logger LOG = LoggerFactory.getLogger(LastAccessCleaner.class);

    protected LastAccessStore<BasicTSKey, BasicTSEntry, LastAccessUpdateMethod> _lastAccessStore;
    
    protected boolean _isTesting;

    public LastAccessCleaner() { }
    
    @SuppressWarnings("unchecked")
    @Override
    protected void init(SharedServiceStuff stuff,
            Stores<?,?> stores,
            ClusterViewByServer cluster,
            AtomicBoolean shutdown)
    {
        super.init(stuff, stores, cluster, shutdown);
        _lastAccessStore = (LastAccessStore<BasicTSKey,BasicTSEntry,LastAccessUpdateMethod>) stores.getLastAccessStore();
        _isTesting = stuff.isRunningTests();
    }
    
    @Override
    protected LastAccessCleanupStats _cleanUp() throws Exception
    {
        final LastAccessCleanupStats stats = new LastAccessCleanupStats();

        if (_lastAccessStore.isClosed()) {
            if (!_isTesting) {
                LOG.warn("LastAccessCleaner task cancelled: last-access has been closed");
            }
            return stats;
        }
        
        final long currentTime = _timeMaster.currentTimeMillis();
        
        _lastAccessStore.scanEntries(new LastAccessIterationCallback() {
                    @Override
                    public IterationAction processEntry(StorableKey key,
                            EntryLastAccessed entry)
                                    throws StoreException {
                        if (!entry.isExpired(currentTime)) {
                            stats.addRemaining(entry.type);
                            return IterationAction.SKIP_ENTRY; // doesnt really matter but...
                        }
                        delete(key);
                        stats.addExpired(entry.type);
                        return IterationAction.PROCESS_ENTRY;
                    }
        });
        return stats;
    }

    private void delete(StorableKey rawKey) throws StoreException
    {
        // TODO: should we add a wait or yield every N deletes?
        try {
            _lastAccessStore.removeLastAccess(rawKey);
        } catch (Exception e) { // should try to pass/convert raw key?
            throw new StoreException.Internal(rawKey, e);
        }
    }
}
