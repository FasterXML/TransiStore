package com.fasterxml.transistore.basic;

import junit.framework.TestCase;

public class BasicTSKeyTest  extends TestCase
{
    protected final BasicTSKeyConverter _keyConverter = BasicTSKeyConverter.defaultInstance();

    public void testSimple()
    {
        BasicTSKey key = _keyConverter.construct("silly/key");
        
        assertEquals("tstore://@silly/key", key.toString());
        assertNull(key.getPartitionIdAsBytes());
    }

    public void testKeyWithPartition()
    {
        final String PARTITION_STR = "gruppo";
        final String PATH = "/another/key";
        BasicTSKey key = _keyConverter.construct(PARTITION_STR, PATH);
        
        assertEquals("tstore://"+PARTITION_STR+"@"+PATH, key.toString());
        assertEquals(PARTITION_STR, key.getPartitionId());
        byte[] b = key.asBytes();
        assertEquals(2 + PARTITION_STR.length() + PATH.length(), b.length);

        BasicTSKey key2 = _keyConverter.construct(b);
        assertEquals(PARTITION_STR, key2.getPartitionId());
        assertEquals("gruppo/another/key", key2.getPartitionAndPath());
    }

    public void testToExternalKeyFormat()
    {
        final String PARTITION_STR = "team1";
        final String PATH = "/dir/file";
        BasicTSKey key = _keyConverter.construct(PARTITION_STR, PATH);

        assertEquals("tstore://team1@/dir/file", key.toString());
        assertEquals(key.toString(), _keyConverter.keyToString(key));

        BasicTSKey key2 =  _keyConverter.construct("justpath");
        assertEquals("tstore://@justpath", key2.toString());

        assertEquals(key2.toString(), _keyConverter.keyToString(key2));
    }

    public void testFomrExternalKeyFormat()
    {
        BasicTSKey key = _keyConverter.stringToKey("tstore://team1@/dir/file");
        assertEquals("team1", key.getPartitionId());
        assertEquals("/dir/file", key.getPath());

        key = _keyConverter.stringToKey("tstore://@/dir/file2");
        assertNull(key.getPartitionId());
        assertEquals("/dir/file2", key.getPath());
    }
}
