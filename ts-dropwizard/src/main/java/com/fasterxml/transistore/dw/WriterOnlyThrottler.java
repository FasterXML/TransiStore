package com.fasterxml.transistore.dw;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Semaphore;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.*;
import com.fasterxml.storemate.store.backend.IterationResult;

public class WriterOnlyThrottler
    extends StoreOperationThrottler
{
    protected final StoreOperationThrottler _delegatee;

    /**
     * Let's start with a very simple mutex for PUTs.
     */
    protected final Semaphore _putLock = new Semaphore(1, false);

    /**
     * And ditto for file-system reads: needs to improved in future,
     * but for now this will have to do.
     */
    protected final Semaphore _readLock = new Semaphore(2, false);

    /**
     * Same also applies to file-system writes.
     */
    protected final Semaphore _writeLock = new Semaphore(2, false);

    public WriterOnlyThrottler(StoreOperationThrottler delegatee)
    {
        _delegatee = delegatee;
    }

    /*
    /**********************************************************************
    /* Metadata access
    /**********************************************************************
     */

    @Override
    public long getOldestInFlightTimestamp() {
        return (_delegatee == null) ? 0 : _delegatee.getOldestInFlightTimestamp();
    }

    @Override
    public int getInFlightWritesCount() {
        return (_delegatee == null) ? 0 : _delegatee.getInFlightWritesCount();
    }

    /*
    /**********************************************************************
    /* Operation throttling
    /**********************************************************************
     */
    
    @Override
    public Storable performGet(StoreOperationCallback<Storable> cb,
            long operationTime, StorableKey key)
        throws IOException, StoreException
    {
        return _delegatee.performGet(cb, operationTime, key);
    }

    @Override
    public IterationResult performList(StoreOperationCallback<IterationResult> cb,
            long operationTime)
        throws IOException, StoreException
    {
        return _delegatee.performList(cb, operationTime);
    }
    
    @Override
    public StorableCreationResult performPut(StoreOperationCallback<StorableCreationResult> cb,
            long operationTime, StorableKey key, Storable value)
        throws IOException, StoreException
    {
        try {
            _putLock.acquire();
        } catch (InterruptedException e) {
            throw new StoreException.ServerTimeout(key, "PUT operation interrupted");
        }
        try {
            return _delegatee.performPut(cb, operationTime, key, value);
        } finally {
            _putLock.release();
        }
    }

    @Override
    public Storable performSoftDelete(StoreOperationCallback<Storable> cb,
            long operationTime, StorableKey key)
        throws IOException, StoreException
    {
        return _delegatee.performSoftDelete(cb, operationTime, key);
    }

    @Override
    public Storable performHardDelete(StoreOperationCallback<Storable> cb,
            long operationTime, StorableKey key)
        throws IOException, StoreException
    {
        return _delegatee.performHardDelete(cb, operationTime, key);
    }

    /*
    /**********************************************************************
    /* Filesystem access throttling
    /**********************************************************************
     */

    @Override
    public <T> T performFileRead(FileOperationCallback<T> cb,
            long operationTime, Storable value, File externalFile)
        throws IOException, StoreException
    {
        try {
            _readLock.acquire();
        } catch (InterruptedException e) {
            throw new StoreException.ServerTimeout((value == null) ? null : value.getKey(),
                    "File read operation interrupted");
        }
        try {
            return _delegatee.performFileRead(cb, operationTime, value, externalFile);
        } finally {
            _readLock.release();
        }
    }

    @Override
    public <T> T performFileWrite(FileOperationCallback<T> cb,
            long operationTime, StorableKey key, File externalFile)
        throws IOException, StoreException
    {
        try {
            _writeLock.acquire();
        } catch (InterruptedException e) {
            throw new StoreException.ServerTimeout(key,
                    "File write operation interrupted");
        }
        try {
            return _delegatee.performFileWrite(cb, operationTime, key, externalFile);
        } finally {
            _writeLock.release();
        }
    }

}
