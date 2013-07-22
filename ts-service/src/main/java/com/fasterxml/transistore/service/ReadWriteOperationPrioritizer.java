package com.fasterxml.transistore.service;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Helper class used by {@link BasicTSOperationThrottler} to implement
 * throttling logic for contested read/write operations (mostly for
 * file system, although theoretically also for DBs if necessary).
 */
public final class ReadWriteOperationPrioritizer
{
    private final static double READ_RATIO = 2.0 / 3.0;

    private final static double WRITE_RATIO = 1.0 - READ_RATIO;
    
    protected final Operation _reads;

    protected final Operation _writes;

    protected final int _maxConcurrentThreads;

    protected final Object SCHEDULE_LOCK = new Object();

    protected final Lease READ_LEASE;

    protected final Lease WRITE_LEASE;
    
    public ReadWriteOperationPrioritizer()
    {
        _reads = new Operation("Read", 2, 6, READ_RATIO);
        _writes = new Operation("Write", 2, 5, WRITE_RATIO);
        _maxConcurrentThreads = 8;

        READ_LEASE = new Lease(SCHEDULE_LOCK, _reads, _writes);
        WRITE_LEASE = new Lease(SCHEDULE_LOCK, _writes, _reads);
    }

    public final Lease obtainReadLease() throws InterruptedException {
        _obtainLease(_reads, _writes);
        return READ_LEASE;
    }

    public final Lease obtainWriteLease() throws InterruptedException {
        _obtainLease(_writes, _reads);
        return WRITE_LEASE;
    }

    private void _obtainLease(Operation operation, Operation other)
        throws InterruptedException
    {
        CountDownLatch latch;

        synchronized (SCHEDULE_LOCK) {
            // First: perhaps we have uncontested operations?
            if (operation.canProceedWithoutQueueing(_maxConcurrentThreads, other)) {
                return;
            }
            // If not, queue it up...
            latch = operation.queueOperation();
        }

        // ... and wait for it to obtain the lease that way...
        latch.await();
    }
    
    /*
    /**********************************************************************
    /* Helper classes: lease handling
    /**********************************************************************
     */
    
    /**
     * Objects used for returning leases.
     */
    final static class Lease
    {
        protected final Object _lock;
        
        protected final Operation _primary;
        
        protected final Operation _secondary;

        public Lease(Object lock, Operation prim, Operation sec)
        {
            _lock = lock;
            _primary = prim;
            _secondary = sec;
        }

        public void returnLease() {
            synchronized (_lock) {
                
            }
            // TODO: reduce active count
            ;
        }
    }
    
    /*
    /**********************************************************************
    /* Helper classes: operation modelling
    /**********************************************************************
     */
    
    final static class Operation
    {
        /**
         * Just need to have enough room for any number of elements ever.
         */
        private final static int MAX_QUEUED = 600;

        public final String _desc;
        
        public final int _guaranteedOperations;
        
        public final int _maxOperations;

        /**
         * Relative weight of operations of this type, normalized to be
         * within range of 0.0 and 1.0.
         */
        public final double _weight;
        
        /**
         * Number of operations being actively executed currently.
         */
        protected final AtomicInteger _activeCount = new AtomicInteger(0);

        protected final ArrayBlockingQueue<CountDownLatch> _queued;
        
        public Operation(String desc, int guar, int max, double w)
        {
            _desc = desc;
            _guaranteedOperations = guar;
            _maxOperations = max;
            _weight = w;
            _queued = new ArrayBlockingQueue<CountDownLatch>(MAX_QUEUED);
        }

        public int getActive() {
            return _activeCount.get();
        }

        public CountDownLatch queueOperation() {
            CountDownLatch latch = new CountDownLatch(1);
            if (!_queued.offer(latch)) { // should never occur
                throw new IllegalStateException("INTERNAL ERROR: Can not queue more "
                        +_desc+" operations, queue full: "+_queued);
            }
            return latch;
        }
        
        public boolean canProceedWithoutQueueing(int maxConcurrent,
                Operation otherQueue)
        {
            if (_queued.isEmpty()) {
                int count = _activeCount.get();
                // guaranteed slots are free for taking
                if (count <= _guaranteedOperations) {
                    _activeCount.addAndGet(1);
                    return true;
                }
                // otherwise, perhaps we can just use "at-large" slots?
                // but not beyond max per operation
                if (count >= _maxOperations) {
                    return false;
                }
                int total = count + otherQueue.getActive();
                if (total >= maxConcurrent) {
                    return false;
                }
                _activeCount.addAndGet(1);
                return true;
            }
            return false;
        }
    }
}
