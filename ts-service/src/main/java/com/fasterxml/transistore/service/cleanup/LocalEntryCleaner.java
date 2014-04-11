package com.fasterxml.transistore.service.cleanup;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.*;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.*;
import com.fasterxml.storemate.store.backend.IterationAction;
import com.fasterxml.storemate.store.backend.IterationResult;
import com.fasterxml.storemate.store.backend.StorableLastModIterationCallback;
import com.fasterxml.storemate.store.lastaccess.LastAccessStore;
import com.fasterxml.storemate.store.lastaccess.LastAccessUpdateMethod;
import com.fasterxml.clustermate.service.*;
import com.fasterxml.clustermate.service.cleanup.CleanupTask;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;
import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.service.BasicTSEntry;

/**
 * Helper class used to keep track of clean up progress
 * for local metadata entry cleanup.
 */
public class LocalEntryCleaner
    extends CleanupTask<LocalCleanupStats>
{
    protected final Logger LOG;

    /**
     * Time-to-live for tomb stones
     */
    protected long _tombstoneTTLMsecs;

    protected StorableStore _entryStore;

    protected LastAccessStore<BasicTSKey,BasicTSEntry,LastAccessUpdateMethod> _lastAccessStore;

    protected StoredEntryConverter<BasicTSKey,BasicTSEntry,?> _entryConverter;
    
    protected boolean _isTesting;
    
    public LocalEntryCleaner() {
        this(LoggerFactory.getLogger(LocalEntryCleaner.class));
    }

    public LocalEntryCleaner(Logger log) {
        LOG = log;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    protected void init(SharedServiceStuff stuff,
            Stores<?,?> stores,
            ClusterViewByServer cluster,
            AtomicBoolean shutdown)
    {
        super.init(stuff, stores, cluster, shutdown);
        _tombstoneTTLMsecs = stuff.getServiceConfig().cfgTombstoneTTL.getMillis();
        _entryConverter = stuff.getEntryConverter();
        _entryStore = stores.getEntryStore();
        _lastAccessStore = (LastAccessStore<BasicTSKey, BasicTSEntry,LastAccessUpdateMethod>) stores.getLastAccessStore();
        _isTesting = stuff.isRunningTests();
    }

    @Override
    protected void prepareForStop()
    {
        // Could we do something here?
        super.prepareForStop();
    }

    @Override
    protected LocalCleanupStats _cleanUp() throws Exception
    {
        final LocalCleanupStats stats = new LocalCleanupStats();
        try {
            boolean fullyCompleted = _cleanUp0(stats);
            _reportEndSuccess(stats, fullyCompleted);
            return stats;
        } catch (Exception e) {
            _reportEndFail(stats, e);
            throw e;
        }
    }

    protected boolean _cleanUp0(final LocalCleanupStats stats) throws Exception
    {
        if (shouldStop()) { // shouldn't get called if so but...
            _reportProblem("LocalEntryCleanup task called when 'shouldStop()' returns false: should not happen; quitting");
            return false;
        }
        if (_entryStore.isClosed()) {
            if (!_isTesting) {
                _reportProblem("LocalEntryCleanup task cancelled: Entry DB has been closed");
            }
            return false;
        }
        _reportStart();

        final ThrottlingDeleter deleter = _constructDeleter(stats);
        
        final long tombstoneThreshold = _timeMaster.currentTimeMillis() - _tombstoneTTLMsecs;
        IterationResult r = _entryStore.iterateEntriesByModifiedTime(StoreOperationSource.CLEANUP, null,
                0L, new StorableLastModIterationCallback() {
            @Override
            public IterationAction verifyTimestamp(long timestamp) {
                return IterationAction.PROCESS_ENTRY;
            }

            @Override
            public IterationAction verifyKey(StorableKey key)
            {
                // first things first: do we need to quit?
                // TODO: maybe consider max runtime?
                if (shouldStop()) {
                    _reportProblem("Stopping "+LocalEntryCleaner.class.getName()+" early due to shutdown");
                    return IterationAction.TERMINATE_ITERATION;
                }
                return IterationAction.PROCESS_ENTRY;
            }

            @Override
            public IterationAction processEntry(Storable raw) throws StoreException
            {
                // for tombstones easy, common max-TTL:
                final StoredEntry<BasicTSKey> entry = _entryConverter.entryFromStorable(raw);
                if (raw.isDeleted()) {
                    if (entry.insertedBefore(tombstoneThreshold)) {
                        deleter.deleteTombstone(entry);
                        stats.addExpiredTombstone();
                        return IterationAction.PROCESS_ENTRY;
                    }
                    stats.addRemainingTombstone();
                } else {
                    // for other entries bit more complex; basically checking following possibilities:
                    // (a) Entry is older than its maxTTL (which varies entry by entry), can be removed
                    // (b) Entry is younger than its minTTL since creation, can be skipped
                    // (c) Entry needs to be retained based on local last-access time: skip
                    // (d) Must check global last-access to determine whether to keep or skip
                    final long currentTime = _timeMaster.currentTimeMillis();
                    if (entry.hasExceededMaxTTL(currentTime)) { // (a) remove
                        stats.addExpiredMaxTTLEntry();
                        deleter.deleteExpired(entry);
                        return IterationAction.PROCESS_ENTRY;
                    }
                    if (!entry.hasExceededMinTTL(currentTime)) { // (b) skip
                        stats.addRemainingEntry();
                    } else if (!entry.usesLastAccessTime()) { // no last-access time check; retain
                        stats.addRemainingEntry();
                    } else { // do need to verify last-access info...
                        if (!entry.hasExceededLastAccessTTL(currentTime,
                                _lastAccessStore.findLastAccessTime(entry.getKey(), entry.getLastAccessUpdateMethod()))) {
                            stats.addRemainingEntry(); // (c) keep
                        } else { // (d): add to list of things to check...
                            // !!! TODO
                            stats.addRemainingEntry();
                        }
                    }
                }
                // to effect throttling, need to denote read-only entry
                deleter.skippedEntry(entry);
                return IterationAction.PROCESS_ENTRY;
            }
        });

        deleter.finish();
        
        return (r == IterationResult.FULLY_ITERATED);
    }

    /*
    /**********************************************************************
    /* Overridable reporting methods
    /**********************************************************************
     */

    protected ThrottlingDeleter _constructDeleter(LocalCleanupStats stats) {
        return new ThrottlingDeleter(LOG, _entryStore, stats, _shutdown);
    }
    
    protected void _reportStart()
    {
        if (LOG != null) {
            LOG.info("Starting local entry cleanup: will remove tombstones older than {}",
                    TimeMaster.timeDesc(_tombstoneTTLMsecs));
        }
    }

    protected void _reportProblem(String msg)
    {
        if (LOG != null) {
            LOG.warn(msg);
        }
    }
    
    protected void _reportEndFail(LocalCleanupStats stats, Exception e)
    {
        if (LOG != null) {
            LOG.info("Failed the local entry cleanup, problem (of type {}): {}. Results: {}",
                    e.getClass().getName(), e.getMessage(), stats);
        }
    }
    
    protected void _reportEndSuccess(LocalCleanupStats stats, boolean fullyCompleted)
    {
        if (LOG != null) {
            if (fullyCompleted) {
                LOG.info("Completed local entry cleanup: {}", stats);
            } else {
                LOG.info("Terminated local entry cleanup, after partial processing: {}", stats);
            }
        }
    }

}
