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
        final String PARTITION_STR = "gruppo";
        final String PATH = "/another/key";
        BasicTSKey key = _keyConverter.construct(PARTITION_STR, PATH);
        
        assertEquals("6:"+PARTITION_STR+PATH, key.toString());
        assertEquals(PARTITION_STR, key.getPartitionId());
        byte[] b = key.asBytes();
        assertEquals(2 + PARTITION_STR.length() + PATH.length(), b.length);

        BasicTSKey key2 = _keyConverter.construct(b);
        assertEquals(PARTITION_STR, key2.getPartitionId());
        assertEquals("gruppo/another/key", key2.getExternalPath());
    }
}
