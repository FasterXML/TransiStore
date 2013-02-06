package com.fasterxml.transistore.cmd;

import java.io.File;

import junit.framework.TestCase;

public class TestCommandUtils extends TestCase
{
    public void testFilePath()
    {
        File f = new File("stuff.txt");
        assertEquals("/stuff.txt", TStoreCmdBase.pathFromFile(f));
        f = new File(new File("foo"), "bar.txt");
        assertEquals("/foo/bar.txt", TStoreCmdBase.pathFromFile(f));

        // Unix parent/current dir refs should be ignored from such paths:
        assertEquals("/Foobar.txt", TStoreCmdBase.pathFromFile(new File("../../Foobar.txt")));
        assertEquals("/bar/foo.txt", TStoreCmdBase.pathFromFile(new File("../bar/foo.txt")));
    }
}
