package com.fasterxml.transistore.service;

import com.fasterxml.storemate.store.lastaccess.LastAccessUpdateMethod;

/**
 * Standard set of {@link LastAccessUpdateMethod} choices that "Basic"
 * TransiStore implementation supports -- simply just "none" or "simple";
 * latter meaning that there is one-to-one mapping between stored entries
 * and matching last-access timestamps.
 *<p>
 * NOTE: no real support is implemented for using last-accessed
 * functionality.
 */
public enum TSLastAccess implements LastAccessUpdateMethod
{
    NONE(0), SIMPLE(1);
    
    private final int _index;

    private TSLastAccess(int index) {
        _index = index;
    }

    @Override public int asInt() { return _index; }
    @Override public byte asByte() { return (byte) _index; }
    @Override public boolean meansNoUpdate() { return (this == NONE); }

    public static TSLastAccess valueOf(int v)
    {
        if (v == NONE._index) {
            return NONE;
        }
        if (v == SIMPLE._index) {
            return SIMPLE;
        }
        return null;
    }
}
