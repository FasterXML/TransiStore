package com.fasterxml.transistore.basic;

/**
 * Enumeration for standard paths recognized by the basic TransiStore implementation
 */
public enum BasicTSPath
{
    // access to stored entries
    STORE_ENTRY, // single-entry CRUD
    STORE_ENTRY_INFO, // metadata about single entry
    STORE_ENTRIES, // multi-entry listings
    STORE_STATUS, // diagnostics interface

    // re-routing store access
    STORE_FIND_ENTRY, // like STORE_ENTRY, but re-routes if necessary
    STORE_FIND_LIST, // like STORE_LIST, but re-routes if necessary
    
    // access to node status: GET for status, PUT for update, POST for hello/bye
    NODE_STATUS,
    // and various metrics: just GET (for now?)
    NODE_METRICS,

    // access to sync information
    SYNC_LIST, // request for change list (ids)
    SYNC_PULL, // request for specific (changed/new) entries
    
    ;
}
