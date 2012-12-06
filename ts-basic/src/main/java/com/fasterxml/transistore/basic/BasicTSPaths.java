package com.fasterxml.transistore.basic;

import com.fasterxml.clustermate.api.PathType;
import com.fasterxml.clustermate.api.DecodableRequestPath;
import com.fasterxml.clustermate.api.RequestPathBuilder;
import com.fasterxml.clustermate.api.RequestPathStrategy;

/**
 * Vagabond-specific implementation of {@link RequestPathStrategy}.
 *<p>
 * Paths are mapped as follows:
 *<ul>
 * <li>Store entries under ".../store/":
 *  <ul>
 *    <li>".../store/entry" for single-entry</li>
 *    <li>".../store/list" for range access</li>
 *  </ul>
 * <li>Node status entries under ".../node/":
 *  <ul>
 *    <li>".../node/status" for basic status</li>
 *  </ul>
 *</ul>
 * <li>Sync entries under ".../sync/":
 *  <ul>
 *    <li>".../sync/list" for accessing metadata for changes</li>
 *    <li>".../sync/pull" for pulling entries to sync</li>
 *  </ul>
 *</ul>
 */
@SuppressWarnings("unchecked")
public class BasicTSPaths extends RequestPathStrategy
{
    
    protected final static String FIRST_SEGMENT_STORE = "store";
    protected final static String FIRST_SEGMENT_NODE = "node";
    protected final static String FIRST_SEGMENT_SYNC = "sync";

    protected final static String SECOND_SEGMENT_STORE_ENTRY = "entry";
    protected final static String SECOND_SEGMENT_STORE_LIST = "list";

    protected final static String SECOND_SEGMENT_NODE_STATUS = "status";

    protected final static String SECOND_SEGMENT_SYNC_LIST = "list";
    protected final static String SECOND_SEGMENT_SYNC_PULL = "pull";
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Path building
    ///////////////////////////////////////////////////////////////////////
     */
    
    @Override
    public <K extends RequestPathBuilder> K appendStoreEntryPath(K nodeRoot) {
        return (K) _storePath(nodeRoot).addPathSegment(SECOND_SEGMENT_STORE_ENTRY);
    }

    @Override
    public <K extends RequestPathBuilder> K appendStoreListPath(K nodeRoot) {
        return (K) _storePath(nodeRoot).addPathSegment(SECOND_SEGMENT_STORE_LIST);
    }

    @Override
    public <K extends RequestPathBuilder> K appendNodeStatusPath(K nodeRoot) {
        return (K) _nodePath(nodeRoot).addPathSegment(SECOND_SEGMENT_NODE_STATUS);
    }

    @Override
    public <K extends RequestPathBuilder> K appendSyncListPath(K nodeRoot) {
        return (K) _syncPath(nodeRoot).addPathSegment(SECOND_SEGMENT_SYNC_LIST);
    }

    @Override
    public <K extends RequestPathBuilder> K appendSyncPullPath(K nodeRoot) {
        return (K) _syncPath(nodeRoot).addPathSegment(SECOND_SEGMENT_SYNC_PULL);
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Path matching (decoding)
    ///////////////////////////////////////////////////////////////////////
     */

    @Override
    public PathType matchPath(DecodableRequestPath pathDecoder)
    {
        String full = pathDecoder.getPath();
        if (pathDecoder.matchPathSegment(FIRST_SEGMENT_STORE)) {
            if (pathDecoder.matchPathSegment(SECOND_SEGMENT_STORE_ENTRY)) {
                return PathType.STORE_ENTRY;
            }
            if (pathDecoder.matchPathSegment(SECOND_SEGMENT_STORE_LIST)) {
                return PathType.STORE_LIST;
            }
        } else if (pathDecoder.matchPathSegment(FIRST_SEGMENT_NODE)) {
            if (pathDecoder.matchPathSegment(SECOND_SEGMENT_NODE_STATUS)) {
                return PathType.NODE_STATUS;
            }
        } else if (pathDecoder.matchPathSegment(FIRST_SEGMENT_SYNC)) {
            if (pathDecoder.matchPathSegment(SECOND_SEGMENT_SYNC_LIST)) {
                return PathType.SYNC_LIST;
            }
            if (pathDecoder.matchPathSegment(SECOND_SEGMENT_SYNC_PULL)) {
                return PathType.SYNC_PULL;
            }
        }
        // if no match, need to reset
        pathDecoder.setPath(full);
        return null;
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Internal methods
    ///////////////////////////////////////////////////////////////////////
     */
    
    protected <K extends RequestPathBuilder> K _storePath(K nodeRoot) {
        return (K) nodeRoot.addPathSegment(FIRST_SEGMENT_STORE);
    }

    protected <K extends RequestPathBuilder> K _nodePath(K nodeRoot) {
        return (K) nodeRoot.addPathSegment(FIRST_SEGMENT_NODE);
    }

    protected <K extends RequestPathBuilder> K _syncPath(K nodeRoot) {
        return (K) nodeRoot.addPathSegment(FIRST_SEGMENT_SYNC);
    }
}
