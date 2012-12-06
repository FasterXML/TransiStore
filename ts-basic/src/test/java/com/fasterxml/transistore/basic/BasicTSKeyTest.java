package com.fasterxml.transistore.basic;

import junit.framework.TestCase;

public class BasicTSKeyTest  extends TestCase
{
    protected final BasicTSKeyConverter _keyConverter = BasicTSKeyConverter.defaultInstance();

    public void testSimple()
    {
        BasicTSKey key = _keyConverter.construct("silly/key");
        
        assertEquals("0:silly/key", key.toString());
        assertNull(key.getPartitionIdAsBytes());
    }

    public void testKeyWithGroup()
    {
        BasicTSKey key = _keyConverter.construct("gruppo", "/another/key");
        
        assertEquals("6:gruppo/another/key", key.toString());
        assertEquals("gruppo", key.getPartitionId());
    }
}
