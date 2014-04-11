package com.fasterxml.transistore.service.cleanup;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;

import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.*;
import com.fasterxml.transistore.basic.BasicTSKey;

/**
 * Helper class to encapsulate details of throttling process of deleting local
 * entries, to try to reduce negative performance effects of background
 * processing.
 */
public class ThrottlingDeleter
{
    // // // Configs: may want to make externally configurable?

    /**
     * We shall batch writes (deletions) to make them more efficient;
     * but cap maximum batch size so that a break is taken after
     * a batch is written (if none taken during batch)
     */
    private final int WRITES_TO_BATCH = 500;

    /**
     * Beyond "expected" failures due to secondary index corruption
     * (which BDB-JE can cause with non-clean shutdowns), let's also
     * allow some number of other failures, before we give up on
     * clean up thread.
     */
    private final int EXCEPTIONS_TO_SKIP = 50;
    
    /**
     * Regardless of how long things take, we'll take a break
     * after reading this number of entries.
     */
    private final int MAX_READS_BEFORE_BREAK = 5000;

    /**
     * Amount of time that we should process until taking a brief
     * break.
     */
    private final long MSECS_UNTIL_BREAK = 200L;

    private final long MIN_BREAK_MSECS = 20L;
    private final long MAX_BREAK_MSECS = 500L;
    
    /*
    /**********************************************************************
    /* Basic configuration
    /**********************************************************************
     */

    private final Logger LOG;

    private final StorableStore _entryStore;
 
    private final LocalCleanupStats _stats;
    
    private final AtomicBoolean _shutdown;

    /*
    /**********************************************************************
    /* Throttling state
    /**********************************************************************
     */

    private long _nextBreak;

    private int _readsSinceBreak;
    
    private int _caughtExceptions;

    private final StoredEntry<?>[] _toDelete;

    private int _toDeleteSize;
    
    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */
    
    public ThrottlingDeleter(Logger log, StorableStore store,
            LocalCleanupStats stats, AtomicBoolean shutdown)
    {
        LOG = log;
        _entryStore = store;
        _stats = stats;
        _nextBreak = System.currentTimeMillis() + MSECS_UNTIL_BREAK;
        _toDelete = new StoredEntry<?>[WRITES_TO_BATCH];
        _shutdown = shutdown;
}

    public void finish() throws StoreException {
        _flushDeletes();
    }
    
    /*
    /**********************************************************************
    /* Public API
    /**********************************************************************
     */
    
    /**
     * Method called for entry that is not being deleted
     */
    public void skippedEntry(StoredEntry<BasicTSKey> entry) {
        if (++_readsSinceBreak < MAX_READS_BEFORE_BREAK) {
            if (((_readsSinceBreak % 16) != 0)
                    || System.currentTimeMillis() < _nextBreak) {
                return;
            }
        }
        _takeABreak();
    }

    public void deleteTombstone(StoredEntry<BasicTSKey> entry) throws StoreException {
        _scheduleDeletion(entry);
    }

    public void deleteExpired(StoredEntry<BasicTSKey> entry) throws StoreException {
        _scheduleDeletion(entry);
    }

    
    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */

    protected void _scheduleDeletion(StoredEntry<BasicTSKey> entry) throws StoreException {
        _toDelete[_toDeleteSize++] = entry;
        if (_toDeleteSize >= WRITES_TO_BATCH) {
            _flushDeletes();
        }
    }

    protected void _takeABreak() {
        final long since = (System.currentTimeMillis() - _nextBreak) + MSECS_UNTIL_BREAK;
        
        // sleep for 25% of time since last break, but with constraints
        long breakMsecs = (since / 4);

        if (breakMsecs < MIN_BREAK_MSECS) {
            breakMsecs = MIN_BREAK_MSECS;
        } else if (breakMsecs > MAX_BREAK_MSECS) {
            breakMsecs = MAX_BREAK_MSECS;
        }
        _stats.addSleep(breakMsecs);
        try {
            Thread.sleep(breakMsecs);
        } catch (InterruptedException e) {
            if (!_shouldStop()) {
                _reportProblem("Unexpected InterruptedException during cleanup; ignoring");
            }
        }
        _nextBreak = System.currentTimeMillis() + MSECS_UNTIL_BREAK;
        _readsSinceBreak = 0;    
    }
    
    protected void _flushDeletes() throws StoreException
    {
        final int len = _toDeleteSize;
        _toDeleteSize = 0;
        int i = 0;

        /* Minor (attempted) optimization: let's sort entries by key, in hopes
         * that this ordering is closer to optimal sequence. Probably won't
         * make much difference but...
         */
        Arrays.sort(_toDelete, 0, len);
        
        try {
            for (; i < len; ++i) {
                StoredEntry<?> entry = _toDelete[i];
                // But since these may take a while, we need to be prepared to bail out...
                if (_shouldStop()) {
                    break;
                }
                _delete(entry);
                if (System.currentTimeMillis() < _nextBreak) {
                    _takeABreak();
                }
            }
        } catch (StoreException e) {
            // Let's try to skip poison pills, but up to some maximum
            if (++_caughtExceptions > EXCEPTIONS_TO_SKIP) {
                _reportProblem("Reached maximum exception count ("+EXCEPTIONS_TO_SKIP+"), will terminate the clean up task");
                throw e;
            }
            StoredEntry<?> entry = _toDelete[i];
            String key = entry.getKey().toString();
            _reportProblem("Caught an exception during deletion of entry "+key+"; will skip "
                    +(len - i)+" remaining entries; problem: "+e);
        }
    }
    
    protected void _delete(StoredEntry<?> entry) throws StoreException
    {
        final StorableKey key = entry.getStorableKey();
        try {
            _entryStore.hardDelete(StoreOperationSource.CLEANUP, null, key, true);
        } catch (StoreException.DB e) {
            /* 26-Sep-2013, tatu: We got some of these in production (for BDB-JE);
             * need to be able to gracefully skip.
             */
            if (e.getType() != StoreException.DBProblem.SECONDARY_INDEX_CORRUPTION) {
                throw e;
            }
            if (++_stats.corruptEntries == 1) { // report only first occurrence
                String keyStr = String.valueOf(entry.getKey());
                _reportProblem("Corrupt entry (key '"+keyStr+"'): need to skip (will only report aggregates after first fail");
            }
        } catch (StoreException e) {
            throw e;
        } catch (IOException e) {
            throw new StoreException.IO(key, e);
        }
    }

    protected boolean _shouldStop() {
        return _shutdown.get();
    }
    
    protected void _reportProblem(String msg)
    {
        if (LOG != null) {
            LOG.warn(msg);
        }
    }
}
