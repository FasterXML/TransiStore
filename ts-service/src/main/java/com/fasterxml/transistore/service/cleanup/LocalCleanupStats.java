package com.fasterxml.transistore.service.cleanup;

/**
 * Helper class used to keep track of clean up progress
 * for local data store (entry metadata, files) cleanup.
 */
public class LocalCleanupStats
{
    // Number tombstones expired
    protected int expiredTombstones = 0;
    // Number of metadata entries removed due to exceeding max TTL
    protected int expiredEntriesMaxTTL = 0;
    // Number of metadata entries removed due to exceeding TTL since last access
    protected int expiredEntriesLastAccess = 0;

    // Number of skipped (non-expired) tombstones
    protected int remainingTombstones = 0;
    // Number of skipped (non-expired) non-tombstone entries
    protected int remainingEntries = 0;

    /**
     * Alas, it may be possible for secondary index to get corrupt (should not,
     * if we are using transactions); if so, count.
     */
    protected int corruptEntries = 0;
    
    // And then "something other"; should not get any hits...
    protected int unknownEntries = 0;

    // Extra sleeps issued during cleanup
    protected long extraSleepMsecs;
    protected int extraSleepIntervals;
    
    public void addExpiredTombstone() { ++expiredTombstones; }
    public void addExpiredMaxTTLEntry() { ++expiredEntriesMaxTTL; }
    public void addExpiredLastAccessEntry() { ++expiredEntriesLastAccess; }

    public void addRemainingTombstone() { ++remainingTombstones; }
    public void addRemainingEntry() { ++remainingEntries; }

    public void addCorruptEntry() { ++corruptEntries; }
    
    public void addUnknownEntry() { ++unknownEntries; }

    public void addSleep(long msecs) {
        extraSleepMsecs += msecs;
        ++extraSleepIntervals;
    }
    
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder(60)
            .append("Removed: ").append(expiredTombstones)
            .append(" expired tombstones, ").append(expiredEntriesMaxTTL)
            .append(" (max-TTL) / ").append(expiredEntriesLastAccess)
            .append(" (last-access) entries; left: ").append(remainingTombstones)
            .append(" tombstones, ").append(remainingEntries)
            .append(" entries");
        if (corruptEntries > 0) {
            sb = sb.append("; had to work around ").append(corruptEntries)
                    .append(" CORRUPT");
        } else {
            sb = sb.append("; had NO corrupt");
        }
        if (unknownEntries > 0) {
            sb = sb.append(" skipped over ").append(unknownEntries).append(" unknown");
        } else {
            sb = sb.append(", NO unknown");
        }
        sb = sb.append(" entries; slept extra ").append(extraSleepIntervals)
                .append("x for ")
                .append(extraSleepMsecs)
                .append(" msecs");
        return sb.toString();
    }
}
