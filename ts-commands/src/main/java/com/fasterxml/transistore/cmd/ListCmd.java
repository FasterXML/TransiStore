package com.fasterxml.transistore.cmd;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectWriter;

import com.fasterxml.clustermate.api.ListItemType;
import com.fasterxml.clustermate.client.operation.ListOperationResult;
import com.fasterxml.clustermate.client.operation.StoreEntryLister;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.basic.BasicTSListItem;
import com.fasterxml.transistore.client.BasicTSClient;

import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;

@Command(name = "list", description = "Lists files stored in TStore, under specified partition."
+"\nOutput format (in text): size/age/max-ttl/entry-key")
public class ListCmd extends TStoreCmdBase
{
    /**
     * By default, let's limit to 1000 entries
     */
    public static final int DEFAULT_MAX_TO_LIST = 1000;
    
    @Option(name = { "-m", "--max" }, description = "Maximum number of entries to list (default: 1000)",
            arity=1 )
    public int maxEntries = DEFAULT_MAX_TO_LIST;

    @Arguments(title="prefix",
            description = "Server-side prefix (partition and optional path prefix) that defines entries to list",
            usage = "[prefix]")
    public List<String> pathInfo;

    public ListCmd() {
        // false -> Not ok to write verbose info on stdout
        super(false);
    }
    
    @Override
    public void run()
    {
        // but also need prefix of some kind
        if ((pathInfo == null) || pathInfo.size() != 1) {
            throw new IllegalArgumentException("Can only take single argument, path prefix for entries to list");
        }
        BasicTSKey prefix = null;
        try {
            prefix = contentKey(pathInfo.get(0));
        } catch (Exception e) {
            System.err.println("Invalid prefix '"+pathInfo.get(0)+"', problem: "+e.getMessage());
            System.exit(1);
        }
        BasicTSClient client = bootstrapClient();

        // as JSON or text?
        ListOperationResult<?> result = null;
        try {
            if (isJSON) {
                result = listAsJSON(client, prefix);
            } else {
                result = listAsText(client, prefix);
            }
        } catch (Exception e) {
            System.err.println("ERROR: ("+e.getClass().getName()+"): "+e.getMessage());
            if (e instanceof RuntimeException || true) {
                e.printStackTrace(System.err);
            }
            System.exit(2);
        }
        client.stop();
        if (result.failed()) {
            System.err.println("Call failure when listing entries: "+result.getFirstFail().getFirstCallFailure());
            System.exit(3);
        }
    }

    private ListOperationResult<?> listAsText(BasicTSClient client, BasicTSKey prefix) throws InterruptedException
    {
        int left = Math.max(1, maxEntries);
        StoreEntryLister<BasicTSKey, BasicTSListItem> lister = client.listContent(null, prefix, ListItemType.fullEntries);
        boolean hasWarned = false;

        while (true) {
            ListOperationResult<BasicTSListItem> result = lister.listMore(Math.min(left, 100));
            if (result.failed()) {
                return result;
            }
            List<BasicTSListItem> entries = result.getItems();
            final long now = System.currentTimeMillis();
            int ix = 0;
            for (BasicTSListItem entry : entries) {
                ++ix;
                System.out.printf("%s %s %s %s\n",
                        size(entry.getLength()), ageMsecs(now - entry.created), ageSecs(entry.maxTTL),
                        contentKey(entry.getKey()).toString());
                if (!hasWarned) {
                    List<String> unknown = entry.unknownProperties();
                    if (!unknown.isEmpty()) {
                        hasWarned = true;
                        warn("Unknown properties for list item entry #"+ix+": "+unknown);
                    }
                }
                --left;
            }
            if (left < 1 || entries.isEmpty()) {
                return result;
            }
        }
    }
    
    private ListOperationResult<?> listAsJSON(BasicTSClient client, BasicTSKey prefix)
            throws InterruptedException, IOException
    {
        int left = Math.max(1, maxEntries);
        StoreEntryLister<BasicTSKey, BasicTSListItem> lister = client.listContent(null, prefix, ListItemType.fullEntries);
        // we'll have to deserialize, serialize back...
        ObjectWriter w = jsonWriter(BasicTSListItem.class);
        // let's wrap output in JSON array (or not?)
        JsonGenerator jgen = w.getFactory().createGenerator(System.out);
        try {
            jgen.writeStartArray();
            ListOperationResult<?> result = null;
            while (true) {
                result = lister.listMore(Math.min(left, 100));
                if (result.failed()) { // note: no closing JSON array for failures
                    return result;
                }
                List<?> items = result.getItems();
                if (items.isEmpty()) {
                    break;
                }
                for (Object item : items) {
                    w.writeValue(jgen, item);
                    // should we force this or not... ?
                    jgen.writeRaw("\n");
                    --left;
                }
                if (left < 1) {
                    break;
                }
            }
            jgen.writeEndArray();
            jgen.writeRaw("\n");
            return result;
        } finally {
            jgen.close();
        }
    }
}
