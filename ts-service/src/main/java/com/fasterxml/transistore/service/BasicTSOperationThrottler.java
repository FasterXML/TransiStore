package com.fasterxml.transistore.service;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Semaphore;

//import com.fasterxml.clustermate.service.StartAndStoppable;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.*;
import com.fasterxml.storemate.store.backend.IterationResult;

/**
 * This is the standard {@link StoreOperationThrottler} to use with
 * TransiStore.
 */
public class BasicTSOperationThrottler
    extends StoreOperationThrottler
//    implements StartAndStoppable
{
    private final static boolean DISABLED = false;

    /**
     * Let's start with a very simple mutex for local DB operations
     * done as part of PUT operations. Since they should be quick,
     * can start with a simple mutex.
     * But allow two concurrent updates, not just one.
     */
    protected final Semaphore _putLock = new Semaphore(2, true);

    /**
     * We may want to throttle reads slightly as well. But should be
     * able to support much higher concurrency than with writes
     */
    protected final Semaphore _getLock = new Semaphore(6, false);

    /**
     * Listings can be pricey as well, so let's throttle to... say,
     * eight as well. But make this fair, since it may take longer
     * than other read access.
     */
    protected final Semaphore _listLock = new Semaphore(8, false);

    /**
     * And ditto for file-system reads: needs to improved in future,
     * but for now this will have to do.
     * Specifically, looks like modern file systems can handle concurrency
     * quite well.
     */
    protected final Semaphore _fsReadLock = new Semaphore(6, true);

    /**
     * Same also applies to file-system writes.
     */
    protected final Semaphore _fsWriteLock = new Semaphore(4, true);

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */
    
    public BasicTSOperationThrottler() { }

    /*
    @Override
    public void start() throws Exception {
    }

    @Override
    public void prepareForStop() throws Exception {
    }

    @Override
    public void stop() throws Exception {
    }
    */

    /*
    /**********************************************************************
    /* Operation throttling
    /**********************************************************************
     */

    @Override
    public Boolean performHas(StoreOperationSource source, long operationTime,
            StorableKey key, StoreOperationCallback<Boolean> cb)
            throws IOException, StoreException
    {
        // For now let's NOT throttle 'has' access... may reconsider in future:
        return cb.perform(operationTime, key, null);
    }
    
    @Override
    public Storable performGet(StoreOperationSource source,
            long operationTime, StorableKey key,
            StoreOperationCallback<Storable> cb)
        throws IOException, StoreException
    {
        if (DISABLED || source != StoreOperationSource.REQUEST) {
            return cb.perform(operationTime, key, null);
        }

        try {
            _getLock.acquire();
        } catch (InterruptedException e) {
            throw new StoreException.ServerTimeout(key, "GET operation interrupted");
        }
        try {
            return cb.perform(operationTime, key, null);
        } finally {
            _getLock.release();
        }
    }

    @Override
    public IterationResult performList(StoreOperationSource source,
            long operationTime, StoreOperationCallback<IterationResult> cb)
        throws IOException, StoreException
    {
        if (DISABLED || source != StoreOperationSource.REQUEST) {
            return cb.perform(operationTime, null, null);
        }

        try {
            _listLock.acquire();
        } catch (InterruptedException e) {
            throw new StoreException.ServerTimeout(null, "List operation interrupted");
        }
        try {
            return cb.perform(operationTime, null, null);
        } finally {
            _listLock.release();
        }
    }
    
    @Override
    public StorableCreationResult performPut(StoreOperationSource source,
            long operationTime, StorableKey key, Storable value,
            StoreOperationCallback<StorableCreationResult> cb)
        throws IOException, StoreException
    {
        if (DISABLED || source != StoreOperationSource.REQUEST) {
            return cb.perform(operationTime, key, value);
        }

        try {
            _putLock.acquire();
        } catch (InterruptedException e) {
            throw new StoreException.ServerTimeout(key, "PUT operation interrupted");
        }
        try {
            return cb.perform(operationTime, key, value);
        } finally {
            _putLock.release();
        }
    }

    /**
     * No additional throttling for (soft) deletions because they are
     * already queued at higher level (applied sequentially).
     */
    @Override
    public Storable performSoftDelete(StoreOperationSource source,
            long operationTime, StorableKey key,
            StoreOperationCallback<Storable> cb)
        throws IOException, StoreException
    {
        return cb.perform(operationTime, key, null);
    }

    /**
     * No additional throttling for hard deletions because they are only
     * done by background batch processes (clean up tasks).
     */
    @Override
    public Storable performHardDelete(StoreOperationSource source,
            long operationTime, StorableKey key,
            StoreOperationCallback<Storable> cb)
        throws IOException, StoreException
    {
        return cb.perform(operationTime, key, null);
    }

    /*
    /**********************************************************************
    /* Filesystem access throttling
    /**********************************************************************
     */

    @Override
    public <T> T performFileRead(StoreOperationSource source,
            long operationTime, Storable value, File externalFile,
            FileOperationCallback<T> cb)
        throws IOException, StoreException
    {
        if (DISABLED) {
            return cb.perform(operationTime, (value == null) ? null : value.getKey(), value, externalFile);
        }

        try {
            _fsReadLock.acquire();
        } catch (InterruptedException e) {
            throw new StoreException.ServerTimeout((value == null) ? null : value.getKey(),
                    "File read operation interrupted");
        }
        try {
            return cb.perform(operationTime, (value == null) ? null : value.getKey(), value, externalFile);
        } finally {
            _fsReadLock.release();
        }
    }

    @Override
    public <T> T performFileWrite(StoreOperationSource source,
            long operationTime, StorableKey key, File externalFile,
            FileOperationCallback<T> cb)
        throws IOException, StoreException
    {
        if (DISABLED || source != StoreOperationSource.REQUEST) {
            return cb.perform(operationTime, key, null, externalFile);
        }

        try {
            _fsWriteLock.acquire();
        } catch (InterruptedException e) {
            throw new StoreException.ServerTimeout(key, "File write operation interrupted");
        }
        try {
            return cb.perform(operationTime, key, null, externalFile);
        } finally {
            _fsWriteLock.release();
        }
    }
}
