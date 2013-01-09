package com.fasterxml.transistore.cmd;

import java.util.*;

import com.fasterxml.storemate.shared.IpAndPort;

/**
 * Skeletal copy of the service configuration, used by local command-line
 * tools to get basic information, but without requiring a dependency to
 * definition (plus ignoring stuff that is not relevant to client side).
 */
public class SkeletalServiceConfig
{
    public Wrapper v = new Wrapper(); // "v" is for Vagabond!
    
    public static class Wrapper {
        public Cluster cluster = new Cluster();
    }

    public static class Cluster
    {
        public int clusterKeyspaceSize;

        // // Static config:

        public List<Node> clusterNodes = new ArrayList<Node>();
    }

    public static class Node
    {
        public IpAndPort ipAndPort;
        public int keyRangeStart;
        public int keyRangeLength;

        public Node() { }
        
        // Alternate constructor for "simple" end point defs:
        public Node(String str) {
            ipAndPort = new IpAndPort(str);
        }
    }
}
