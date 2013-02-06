package com.fasterxml.transistore.cmd;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectWriter;

import com.fasterxml.clustermate.api.ListItemType;
import com.fasterxml.clustermate.api.msg.ListItem;
import com.fasterxml.clustermate.client.operation.ListOperationResult;
import com.fasterxml.clustermate.client.operation.StoreEntryLister;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.client.BasicTSClient;
import com.fasterxml.transistore.client.BasicTSClientConfig;

import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;

@Command(name = "list", description = "Lists files stored in TStore, under specified partition")
public class ListCmd extends TStoreCmdBase
{
    @Option(name = { "-m", "--max" }, description = "Maximum number of entries to list",
            arity=1 )
    public int maxEntries = Integer.MAX_VALUE;

    @Arguments(title="prefix",
            description = "Server-side prefix (partition and optional path prefix) that defines entries to list",
            usage = "[prefix]",
            required=true)
    public List<String> pathInfo;

    @Override
    public void run()
    {
        // start with config file
        SkeletalServiceConfig serviceConfig = getServiceConfig();
        BasicTSClientConfig clientConfig = getClientConfig();

        // but also need prefix of some kind
        if (pathInfo.size() != 1) {
            throw new IllegalArgumentException("Can only take single argument, path prefix for entries to list");
        }
        BasicTSKey prefix = null;
        try {
            prefix = contentKey(pathInfo.get(0));
        } catch (Exception e) {
            System.err.println("Invalid prefix '"+pathInfo.get(0)+"', problem: "+e.getMessage());
            System.exit(1);
        }
        BasicTSClient client = bootstrapClient(clientConfig, serviceConfig);

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
        StoreEntryLister<BasicTSKey, String> lister = client.listContent(prefix, ListItemType.names);
        
        while (true) {
            ListOperationResult<String> result = lister.listMore(Math.min(left, 100));
            if (result.failed()) {
                return result;
            }
            List<String> names = result.getItems();
            for (String name : names) {
                System.out.println(name);
                --left;
            }
            if (left < 1 || names.isEmpty()) {
                return result;
            }
        }
    }

    private ListOperationResult<?> listAsJSON(BasicTSClient client, BasicTSKey prefix)
            throws InterruptedException, IOException
    {
        int left = Math.max(1, maxEntries);
        StoreEntryLister<BasicTSKey, ListItem> lister = client.listContent(prefix, ListItemType.minimalEntries);
        // we'll have to deserialize, serialize back...
        ObjectWriter w = jsonWriter(ListItemType.minimalEntries.getValueType());
        // let's wrap output in JSON array (or not?)
        JsonGenerator jgen = w.getJsonFactory().createGenerator(System.out);
        try {
            jgen.writeStartArray();
            ListOperationResult<?> result = null;
            while (true) {
                result = lister.listMore(Math.min(left, 50));
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
