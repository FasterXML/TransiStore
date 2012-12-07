package com.fasterxml.transistore.service.cfg;

import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.file.DefaultFilenameConverter;

import com.fasterxml.transistore.basic.BasicTSKey;

/**
 * We need a custom converter since basic {@link EntryKey} does not
 * have functionality for generic "as String" functionality.
 * Plus we could make filenames bit more aesthetically pleasing...
 */
public class BasicTSFilenameConverter extends DefaultFilenameConverter
{
    protected final EntryKeyConverter<BasicTSKey> _keyConverter;
    
    public BasicTSFilenameConverter(EntryKeyConverter<BasicTSKey> keyConverter)
    {
        _keyConverter = keyConverter;
    }
    
    @Override
    public StringBuilder appendFilename(StorableKey rawKey, final StringBuilder sb)
    {
        BasicTSKey key = _keyConverter.rawToEntryKey(rawKey);
        final char safeChar = _safeChar;

        // Start with partition id, if any
        final String fullPath = key.getExternalPath();
        final int groupLen = key.getPartitionIdLength();
        if (groupLen > 0) {
            sb.append(groupLen);
        }
        sb.append(':');
        
        for (int i = 0, len = fullPath.length(); i < len; ++i) {
            char c = fullPath.charAt(i);
            if ((c <= 0xFF) && isSafe((byte) c)) {
                sb.append(c);
            } else {
                sb.append(safeChar);
            }
        }
        return sb;
    }
}
