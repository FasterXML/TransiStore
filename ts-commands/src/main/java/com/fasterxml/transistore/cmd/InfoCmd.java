package com.fasterxml.transistore.cmd;

import java.util.*;

import com.fasterxml.clustermate.api.msg.ItemInfo;
import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.NodesForKey;
import com.fasterxml.clustermate.client.call.ReadCallResult;
import com.fasterxml.clustermate.client.operation.InfoOperationResult;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.client.BasicTSClient;

import io.airlift.command.*;

/**
 * Command that works similar to Unix "cat" command, displaying contents of one
 * or more stores, writing them to stdout.
 */
@Command(name = "info", description = "Get information on multi-node status of specific Entry")
public class InfoCmd extends TStoreCmdBase
{
    @Arguments(title="paths",
            description = "Server path(s) for entry to show info for"
            ,usage="[path1] ... [pathN]")
    public List<String> paths;

    public InfoCmd() {
        // false -> not ok to print stuff to stdout
        super(false);
    }
    
    @Override
    public void run()
    {
        if (paths == null || paths.size() == 0) { // or could read from stdin?
            throw new IllegalArgumentException("No entries to fetch info for");
        }
        // and then verify that all paths are valid
        List<BasicTSKey> pathList = new ArrayList<BasicTSKey>(paths.size());
        for (String path : paths) {
            try {
                pathList.add(contentKey(path));
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid path '"+path+"': "+e.getMessage());
            }
        }

        BasicTSClient client = bootstrapClient();
        try {
            final long start = System.nanoTime();
            showInfo(client, pathList);
            if (verbose) {
                double millis = (System.nanoTime() - start) / (1000.0 * 1000.0);
                System.out.printf("(took %.1f msec)", millis);
                System.out.println();
            }
        } catch (Exception e) {
            System.err.println("ERROR: ("+e.getClass().getName()+"): "+e.getMessage());
            if (e instanceof RuntimeException || true) {
                e.printStackTrace(System.err);
            }
            System.exit(2);
        }
        client.stop();
    }

    protected void showInfo(BasicTSClient client, List<BasicTSKey> pathList) throws Exception
    {
        for (BasicTSKey path : pathList) {
            NodesForKey nodes = client.getCluster().getNodesFor(path);
            int expCount = nodes.size();
            System.out.printf("Entry '%s': expect %d copies\n", path.toString(), expCount);
            InfoOperationResult<ItemInfo> resp = client.findInfo(null, path);
            int i = 0;
            for (ReadCallResult<ItemInfo> result : resp) {
                ++i;
                ClusterServerNode node = result.getServer();
                System.out.printf(" copy #%d (%s): ", i, node.getAddress().toString());
                if (result.failed()) {
                    System.out.printf("FAILed to access, problem: %s", result.getFailure());
                } else if (result.hasResult()) {
                    ItemInfo info = result.getResult();
                    Compression comp = info.getCompression();
                    if (comp == null || comp == Compression.NONE) {
                        System.out.printf("length %s", size(info.getLength()).trim());
                    } else {
                        System.out.printf("length %s (%s/%s)",
                                size(info.getLength()).trim(), size(info.getCompressedLength()).trim(),
                                comp);
                    }
                    System.out.printf(", hash %X", info.getHash());
                    System.out.printf(", flags %c/%c/%c",
                            info.isDeleted() ? 'd':'-',
                            info.isInlined() ? 'i':'-',
                            info.isReplica() ? 'r':'-');

                } else {
                    System.out.print("NOT FOUND");
                }
                System.out.println();
            }
        }
    }
}
