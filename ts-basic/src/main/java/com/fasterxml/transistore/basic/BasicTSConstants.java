package com.fasterxml.transistore.basic;

/**
 * Container for constants used with Basic TransiStore configuration.
 */
public interface BasicTSConstants
{
    /**
     * Query parameter used to pass optional <code>partition id</code> of resource;
     * used for grouping files for expiration purposes.
     */
    public final static String TS_QUERY_PARAM_PARTITION_ID = "partition";
}
