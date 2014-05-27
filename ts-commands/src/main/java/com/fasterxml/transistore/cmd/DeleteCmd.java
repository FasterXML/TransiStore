package com.fasterxml.transistore.cmd;

import java.io.IOException;
import java.util.*;

import io.airlift.command.*;

import com.fasterxml.storemate.shared.StorableKey;

import com.fasterxml.clustermate.api.ListItemType;
import com.fasterxml.clustermate.client.NodeFailure;
import com.fasterxml.clustermate.client.operation.*;

import com.fasterxml.transistore.basic.*;
import com.fasterxml.transistore.client.BasicTSClient;

@Command(name = "delete", description = "DELETE entries from TStore")
public class DeleteCmd extends TStoreCmdBase
{
    @Option(name = { "-r", "--recursive"}, description = "Recursive deletion (by prefix)")
    public boolean recursive = false;

    @Option(name = { "-m", "--max" }, description = "Maximum number of entries to delete per path (default: 500)",
            arity=1 )
    public int maxEntries = 500;
    
    @Arguments(title="arguments",
            description = "Path(s) to entry(ies) to delete (if non-recursive); or prefix (if recursive)"
            ,usage="[entry1] ... [entryN]"
            ,required=true)
    public List<String> arguments;

    public DeleteCmd() {
        // true -> Ok to write verbose info on stdout
        super(true);
    }
    
    @Override
    public void run()
    {
        // and then verify that all paths are valid server references
        List<BasicTSKey> paths = new ArrayList<BasicTSKey>();

        for (String argument : arguments) {
            try {
                paths.add(contentKey(argument));
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid server entry/path reference: "+e.getMessage());
            }
        }
        BasicTSClient client = bootstrapClient();
        int total = 0;
        try {
            for (BasicTSKey path : paths) {
                // One check: no recursive deletion without partition id, since it won't work
                // reliably
                if (recursive && !path.hasPartitionId()) {
                    System.err.printf("Can not delete recursively without partition id, skipping: %s\n", path);
                    continue;
                }
                total += recursive ? deleteRecursively(client, path) : deleteSingle(client, path);
            }
        } catch (Exception e) {
            System.err.printf("ERROR: (%s): %s", e.getClass().getName(), e.getMessage());
            if (e instanceof RuntimeException) {
                e.printStackTrace(System.err);
            }
            System.exit(1);
        }
        System.out.printf("Deleted %d files\n", total);
        client.stop();
    }

    protected int deleteSingle(BasicTSClient client, BasicTSKey path)
            throws IOException, InterruptedException
    {
        HeadOperationResult headResult = client.headContent(null, path);
        if (headResult.entryFound()) {
            _deleteSingle(client, path);
            return 1;
        }
        warn("No entry '%s' found: skipping", path);
        return 0;
    }

    protected int deleteRecursively(BasicTSClient client, BasicTSKey path)
            throws IOException, InterruptedException
    {
        int left = Math.max(1, maxEntries);
        StoreEntryLister<BasicTSKey, StorableKey> lister = client.listContent(null, path, ListItemType.ids);
        int gotten = 0;

        while (true) {
            ListOperationResult<StorableKey> result = lister.listMore(Math.min(left, 100));
            if (result.failed()) {
                throw new IllegalArgumentException("Call failure (after "+gotten+" entries) when listing entries: "+result.getFirstFail().getFirstCallFailure());
            }
            List<StorableKey> ids = result.getItems();
            for (StorableKey entry : ids) {
                _deleteSingle(client, contentKey(entry));
            }
            gotten += ids.size();
            left -= ids.size();
            if (left < 1 || ids.isEmpty()) {
                return gotten;
            }
        }
    }

    protected boolean _deleteSingle(BasicTSClient client, BasicTSKey path)
        throws IOException, InterruptedException
    {
        DeleteOperationResult deleteResult = client.deleteContent(null, path)
                .completeOptimally()
                .tryCompleteMaximally()
                .finish();

        if (!deleteResult.succeededOptimally()) {
            if (!deleteResult.succeededMinimally()) {
                throw new IOException("Failed to DELETE '"+path+"': "+deleteResult.getFailCount()
                        +" failed nodes tried -- first error: "+deleteResult.getFirstFail());
            }
            NodeFailure fail = deleteResult.getFirstFail();
            int copies = deleteResult.getSuccessCount();
            if (fail == null) {
                warn("(sub-optimal DELETE, %d copies -- no FAIL info?)\n", copies);
            } else {
                warn("(sub-optimal DELETE, %d copies; %s failed, first fail status: %d)\n",
                        copies, fail.getServer().getAddress(), fail.getFirstCallFailure().getStatusCode());
            }
            return true;
        }
        return false;
    }
}
