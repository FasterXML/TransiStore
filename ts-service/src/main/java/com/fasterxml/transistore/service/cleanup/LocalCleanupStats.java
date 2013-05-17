package com.fasterxml.transistore.service.cleanup;

/**
 * Helper class used to keep track of clean up progress
 * for local BDB cleanup.
 */
public class LocalCleanupStats
{
    // Number tombstones expired
    protected int expiredTombstones = 0;
    // Number of local BDB entries removed due to exceeding max TTL
    protected int expiredEntriesMaxTTL = 0;
    // Number of local BDB entries removed due to exceeding TTL since last access
    protected int expiredEntriesLastAccess = 0;

    // Number of skipped (non-expired) tombstones
    protected int remainingTombstones = 0;
    // Number of skipped (non-expired) non-tombstone entries
    protected int remainingEntries = 0;

    // And then "something other"; should not get any hits...
    protected int unknownEntries = 0;

    public void addExpiredTombstone() { ++expiredTombstones; }
    public void addExpiredMaxTTLEntry() { ++expiredEntriesMaxTTL; }
    public void addExpiredLastAccessEntry() { ++expiredEntriesLastAccess; }

    public void addRemainingTombstone() { ++remainingTombstones; }
    public void addRemainingEntry() { ++remainingEntries; }

    public void addUnknownEntry() { ++unknownEntries; }

    @Override
    public String toString()
    {
        return new StringBuilder(60)
            .append("Removed: ").append(expiredTombstones)
            .append(" expired tombstones, ").append(expiredEntriesMaxTTL)
            .append(" (max-TTL) / ").append(expiredEntriesLastAccess)
            .append(" (last-access) entries; left: ").append(remainingTombstones)
            .append(" tombstones, ").append(remainingEntries)
            .append(" entries and skipped ").append(unknownEntries).append(" unknown entries")
            .toString();
    }

}
