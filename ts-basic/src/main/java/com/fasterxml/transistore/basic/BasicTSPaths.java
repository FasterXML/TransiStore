package com.fasterxml.transistore.basic;

import com.fasterxml.clustermate.api.DecodableRequestPath;
import com.fasterxml.clustermate.api.RequestPathBuilder;
import com.fasterxml.clustermate.api.RequestPathStrategy;

/**
 * {@link RequestPathStrategy} implementation for
 * Basic TransiStore configuration.
 *<p>
 * Paths are mapped as follows:
 *<ul>
 * <li>Store entries under ".../store/":
 *  <ul>
 *    <li>".../store/entry" for single-entry access</li>
 *    <li>".../store/entries" for range (ordered multi-entry) access</li>
 *    <li>".../store/status" for store status (admin interface)</li>
 *    <li>".../store/findEntry" for redirecting single-entry</li>
 *    <li>".../store/findList" for redirecting range access</li>
 *  </ul>
 * <li>Node status entries under ".../node/":
 *  <ul>
 *    <li>".../node/status" for basic status</li>
 *  </ul>
 *  <ul>
 *    <li>".../node/metrics" for various metrics</li>
 *  </ul>
 *</ul>
 * <li>Sync entries under ".../sync/":
 *  <ul>
 *    <li>".../sync/list" for accessing metadata for changes</li>
 *    <li>".../sync/pull" for pulling entries to sync</li>
 *  </ul>
 * <li>Intra-cluster (remotely-called) entries under ".../remote/":
 *  <ul>
 *    <li>".../status" is an alias for "node status" that we map to regular status
 *    <li>".../sync/list" for accessing metadata for remote changes</li>
 *    <li>".../sync/pull" for pulling entries to sync from remote cluster</li>
 *  </ul>
 *</ul>
 */
public class BasicTSPaths extends RequestPathStrategy<BasicTSPath>
{
    protected final static String FIRST_SEGMENT_STORE = "store";
    protected final static String FIRST_SEGMENT_NODE = "node";
    protected final static String FIRST_SEGMENT_SYNC = "sync";
    protected final static String FIRST_SEGMENT_REMOTE = "remote";

    protected final static String SEGMENT_ENTRY = "entry";
    protected final static String SEGMENT_ENTRY_INFO = "entryInfo";
    protected final static String SEGMENT_ENTRIES = "entries";
    protected final static String SEGMENT_STATUS = "status";
    protected final static String SEGMENT_FIND_ENTRY = "findEntry";
    protected final static String SEGMENT_FIND_ENTRIES = "findEntries";

    protected final static String SEGMENT_SYNC = "sync";
    protected final static String SEGMENT_METRICS = "metrics";

    protected final static String SEGMENT_LIST = "list";
    protected final static String SEGMENT_PULL = "pull";

    /*
    /**********************************************************************
    /* Path building, external store requests
    /**********************************************************************
     */

    @Override
    public <B extends RequestPathBuilder<B>> B appendPath(B basePath, BasicTSPath type)
    {
        switch (type) {
        case NODE_METRICS:
            return appendNodeMetricsPath(basePath);
        case NODE_STATUS:
            return appendNodeStatusPath(basePath);

        case STORE_ENTRY:
            return appendStoreEntryPath(basePath);
        case STORE_ENTRY_INFO:
            return appendStoreEntryInfoPath(basePath);
        case STORE_ENTRIES:
            return appendStoreListPath(basePath);

        case STORE_FIND_ENTRY:
            return _storePath(basePath).addPathSegment(SEGMENT_FIND_ENTRY);
        case STORE_FIND_LIST:
            return _storePath(basePath).addPathSegment(SEGMENT_FIND_ENTRIES);
        case STORE_STATUS:
            return  _storePath(basePath).addPathSegment(SEGMENT_STATUS);

        case SYNC_LIST:
            return appendSyncListPath(basePath);
        case SYNC_PULL:
            return appendSyncPullPath(basePath);

        case REMOTE_STATUS:
            return appendNodeStatusPath(basePath);
        case REMOTE_SYNC_LIST:
            return appendRemoteSyncListPath(basePath);
        case REMOTE_SYNC_PULL:
            return appendRemoteSyncPullPath(basePath);
        }
        throw new IllegalStateException();
    }

    /*
    /**********************************************************************
    /* Methods for building basic content access paths
    /**********************************************************************å
     */

    @Override
    public <B extends RequestPathBuilder<B>> B appendStoreEntryPath(B basePath) {
        return _storePath(basePath).addPathSegment(SEGMENT_ENTRY);
    }

    @Override
    public <B extends RequestPathBuilder<B>> B appendStoreEntryInfoPath(B basePath) {
        return _storePath(basePath).addPathSegment(SEGMENT_ENTRY_INFO);
    }

    @Override
    public <B extends RequestPathBuilder<B>> B appendStoreListPath(B basePath) {
        return _storePath(basePath).addPathSegment(SEGMENT_ENTRIES);
    }

    /*
    /**********************************************************************
    /* Path building, server-side sync requests
    /**********************************************************************
     */

    @Override
    public <B extends RequestPathBuilder<B>> B appendSyncListPath(B basePath) {
        return _localSyncPath(basePath).addPathSegment(SEGMENT_LIST);
    }

    @Override
    public <B extends RequestPathBuilder<B>> B appendSyncPullPath(B basePath) {
        return _localSyncPath(basePath).addPathSegment(SEGMENT_PULL);
    }

    @Override
    public <B extends RequestPathBuilder<B>> B appendNodeStatusPath(B basePath) {
        return _nodePath(basePath).addPathSegment(SEGMENT_STATUS);
    }

    @Override
    public <B extends RequestPathBuilder<B>> B appendNodeMetricsPath(B basePath) {
        return _nodePath(basePath).addPathSegment(SEGMENT_METRICS);
    }
    
    @Override
    public <B extends RequestPathBuilder<B>> B appendRemoteSyncListPath(B basePath) {
        return _remoteSyncPath(basePath).addPathSegment(SEGMENT_LIST);
    }

    @Override
    public <B extends RequestPathBuilder<B>> B appendRemoteSyncPullPath(B basePath) {
        return _remoteSyncPath(basePath).addPathSegment(SEGMENT_PULL);
    }

    @Override
    public <B extends RequestPathBuilder<B>> B appendRemoteStatusPath(B basePath) {
        return _remotePath(basePath).addPathSegment(SEGMENT_STATUS);
    }

    /*
    /**********************************************************************
    /* Path matching (decoding)
    /**********************************************************************
     */

    @Override
    public BasicTSPath matchPath(DecodableRequestPath pathDecoder)
    {
        String full = pathDecoder.getPath();
        if (pathDecoder.matchPathSegment(FIRST_SEGMENT_STORE)) {
            if (pathDecoder.matchPathSegment(SEGMENT_ENTRY)) {
                return BasicTSPath.STORE_ENTRY;
            }
            if (pathDecoder.matchPathSegment(SEGMENT_ENTRIES)) {
                return BasicTSPath.STORE_ENTRIES;
            }
            if (pathDecoder.matchPathSegment(SEGMENT_ENTRY_INFO)) {
                return BasicTSPath.STORE_ENTRY_INFO;
            }
        } else if (pathDecoder.matchPathSegment(FIRST_SEGMENT_NODE)) {
            if (pathDecoder.matchPathSegment(SEGMENT_STATUS)) {
                return BasicTSPath.NODE_STATUS;
            }
            if (pathDecoder.matchPathSegment(SEGMENT_METRICS)) {
                return BasicTSPath.NODE_METRICS;
            }
        } else if (pathDecoder.matchPathSegment(FIRST_SEGMENT_SYNC)) {
            if (pathDecoder.matchPathSegment(SEGMENT_LIST)) {
                return BasicTSPath.SYNC_LIST;
            }
            if (pathDecoder.matchPathSegment(SEGMENT_PULL)) {
                return BasicTSPath.SYNC_PULL;
            }
        } else if (pathDecoder.matchPathSegment(FIRST_SEGMENT_REMOTE)) {
            if (pathDecoder.matchPathSegment(SEGMENT_SYNC)) {
                if (pathDecoder.matchPathSegment(SEGMENT_LIST)) {
                    return BasicTSPath.REMOTE_SYNC_LIST;
                }
                if (pathDecoder.matchPathSegment(SEGMENT_PULL)) {
                    return BasicTSPath.REMOTE_SYNC_PULL;
                }
            } else if (pathDecoder.matchPathSegment(SEGMENT_STATUS)) {
                return BasicTSPath.REMOTE_STATUS;
            }
        }
        // if no match, need to reset
        pathDecoder.setPath(full);
        return null;
    }

    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */

    protected <B extends RequestPathBuilder<B>> B _storePath(B nodeRoot) {
        return nodeRoot.addPathSegment(FIRST_SEGMENT_STORE);
    }

    protected <B extends RequestPathBuilder<B>> B _nodePath(B nodeRoot) {
        return nodeRoot.addPathSegment(FIRST_SEGMENT_NODE);
    }

    protected <B extends RequestPathBuilder<B>> B _localSyncPath(B nodeRoot) {
        return nodeRoot.addPathSegment(FIRST_SEGMENT_SYNC);
    }

    protected <B extends RequestPathBuilder<B>> B _remotePath(B nodeRoot) {
        return nodeRoot.addPathSegment(FIRST_SEGMENT_REMOTE);
    }

    protected <B extends RequestPathBuilder<B>> B _remoteSyncPath(B nodeRoot) {
        return _remotePath(nodeRoot).addPathSegment(SEGMENT_SYNC);
    }
}
