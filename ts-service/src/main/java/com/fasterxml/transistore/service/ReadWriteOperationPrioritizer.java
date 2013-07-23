package com.fasterxml.transistore.service;

import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Helper class used by {@link BasicTSOperationThrottler} to implement
 * throttling logic for contested read/write operations (mostly for
 * file system, although theoretically also for DBs if necessary).
 *<p>
 * Basic idea is simply: when things are going smoothly, we will allow
 * a reasonable number of concurrent reads and/or writes to proceed
 * without throttling. But if limit on either is reached, queuing is
 * used to apply specific ratio to try to avoid starving of either
 * reads or writes, by basically fixing ratio in which queue is drained.
 */
public final class ReadWriteOperationPrioritizer
{
	// By default, we will use 2:1 ratio between allowing queued reads vs writes
    private final static double READ_RATIO = 2.0 / 3.0;

    private final static double WRITE_RATIO = 1.0 - READ_RATIO;

    protected final Lease READ_LEASE;

    protected final Lease WRITE_LEASE;
    
    public ReadWriteOperationPrioritizer()
    {
        final Operation reads = new Operation("Read", 3, 6, READ_RATIO);
        final Operation writes = new Operation("Write", 2, 5, WRITE_RATIO);

        final int _maxConcurrentThreads = 8;

        // We will use a global lock for updating state of currently
        // active entries; it is shared by this class and {@link Lease}.
        final Object SCHEDULE_LOCK = new Object();
        
        READ_LEASE = new Lease(SCHEDULE_LOCK, _maxConcurrentThreads, reads, writes, 1);
        WRITE_LEASE = new Lease(SCHEDULE_LOCK, _maxConcurrentThreads, writes, reads, 2);
    }

    public final Lease obtainReadLease() throws InterruptedException {
        return READ_LEASE.obtainLease();
    }

    public final Lease obtainWriteLease() throws InterruptedException {
        return WRITE_LEASE.obtainLease();
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

        protected final int _maxConcurrentThreads;
        
        /**
         * To get statistical distribution based on ratios, we need to use
         * (pseudo)random numbers.
         */
        protected final Random _rnd;
        
        protected final Operation _primary;

        protected final Operation _secondary;

        public Lease(Object lock, int maxConc, Operation prim, Operation sec, long rndSeed)
        {
            _lock = lock;
            _maxConcurrentThreads = maxConc;
            _rnd = new Random(rndSeed);
            _primary = prim;
            _secondary = sec;
        }

        public Lease obtainLease() throws InterruptedException
        {
            CountDownLatch latch;

            synchronized (_lock) {
                // First: perhaps we have uncontested operations?
                if (_primary.canProceedWithoutQueueing(_maxConcurrentThreads, _secondary)) {
                    return this;
                }
                // If not, queue it up...
                latch = _primary.queueOperation();
            }
            // ... and wait for it to obtain the lease that way...
            latch.await();
            return this;
        }
        
        public void returnLease() {
            synchronized (_lock) {
            	int primaryCount = _primary.markCompleted();

            	boolean q1 = _primary.couldReleaseQueued(primaryCount);
                boolean q2 = _secondary.couldReleaseQueued();
            	
            	/* 23-Jul-2013, tatu: I don't think there's strict need to loop here;
            	 *  but let's play it safe and do that.
            	 */
            	do {
	
	                if (q1) { // has an entry in primary queue
	                	if (q2) { // and in secondary -- choose one, with bias:
	                		double rnd = _rnd.nextDouble();
	                		if (rnd < _primary.getWeight()) {
	                    		_primary.releaseFromQueue();
	                		} else {
	                			_secondary.releaseFromQueue();
	                		}
	                	} else { // no, just primary
	                		_primary.releaseFromQueue();
	                	}
	                } else if (q2) { // none in primary, but got secondary
	                	_secondary.releaseFromQueue();
	                } else {
	                	break;
	                }
	                q1 = _primary.couldReleaseQueued();
	                q2 = _secondary.couldReleaseQueued();
            	} while (q1 | q2);
            }
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

        public double getWeight() {
        	return _weight;
        }

        public int getActive() {
            return _activeCount.get();
        }

        public CountDownLatch queueOperation()
        {
            CountDownLatch latch = new CountDownLatch(1);
            if (!_queued.offer(latch)) { // should never occur
                throw new IllegalStateException("INTERNAL ERROR: Can not queue more "
                        +_desc+" operations, queue full: "+_queued);
            }
            return latch;
        }

        /**
         * Method called when Operation of this type completed; just needs to
         * subtract counter.
         */
        public int markCompleted() {
        	return _activeCount.addAndGet(-1);
        }
        
        public boolean isQueueEmpty() {
        	return _queued.isEmpty();
        }

        public boolean couldReleaseQueued(int currentCount) {
        	if (isQueueEmpty()) { // nothing queued, nothing to release
        		return false;
        	}
        	// but also need to have room for one more:
        	return currentCount < _maxOperations;
        }

        public boolean couldReleaseQueued()
        {
        	if (isQueueEmpty()) { // nothing queued, nothing to release
        		return false;
        	}
        	// but also need to have room for one more:
        	return _activeCount.get() < _maxOperations;
        }

        public boolean canProceedWithoutQueueing(int maxConcurrent,
                Operation otherQueue)
        {
        	// First rule: both queues must be empty, before proceeding
            if (isQueueEmpty() && otherQueue.isQueueEmpty()) {
                int count = _activeCount.get();

                // guaranteed slots are free for taking
                if (count >= _guaranteedOperations) {
	                // otherwise, perhaps we can just use "at-large" slots?
	                // but not beyond max per operation
	                if (count >= _maxOperations) {
	                    return false;
	                }
	                int total = count + otherQueue.getActive();
	                if (total >= maxConcurrent) {
	                    return false;
	                }
                }
                // Either way, yes, we are ready to proceed:
                _activeCount.addAndGet(1);
                return true;
            }
            return false;
        }

        public int releaseFromQueue()
        {
        	CountDownLatch l = _queued.poll();
        	if (l == null) { // sanity check; should never occur
                throw new IllegalStateException("INTERNAL ERROR: failed to release from queue of "
                        +_desc+" operations, queue empty");
        	}
        	int count = _activeCount.addAndGet(1);
        	l.countDown();
        	return count;
        }
    }
}
