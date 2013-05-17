package com.fasterxml.transistore.service.cleanup;

/**
 * Helper class used to keep track of clean up progress
 * for (local) last-access entry cleanup.
 */
public class LastAccessCleanupStats
{
    // Number of last-accessed entries expired and removed
    protected int expiredSimpleEntries = 0;

    // Number of skipped (non-expired)  entries
    protected int remainingSimpleEntries = 0;

    public void addExpired(byte type) {
        ++expiredSimpleEntries;
    }

    public void addRemaining(byte type) {
        ++remainingSimpleEntries;
    }

    @Override
    public String toString()
    {
        return new StringBuilder(60)
            .append("Removed: ").append(expiredSimpleEntries)
            .append(", left: ").append(remainingSimpleEntries)
            .append(" last-accessed entries")
            .toString();
    }
}
