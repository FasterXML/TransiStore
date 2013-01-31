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

    @Arguments(description = "Partition (and optional name prefix) to list",
            required=true)
    public List<String> pathInfo;

    @Override
    public void run()
    {
        // start with config file
        SkeletalServiceConfig serviceConfig = getServiceConfig();
        BasicTSClientConfig clientConfig = getClientConfig();

        // but also need at least partition
        if (pathInfo.size() > 2) {
            throw new IllegalArgumentException("Too many arguments: can only use 2 (partition, optional prefix)");
        }
        String partition = pathInfo.get(0);
        String pathPrefix = (pathInfo.size() < 2) ? null : pathInfo.get(1);
        BasicTSClient client = bootstrapClient(clientConfig, serviceConfig);

        // as JSON or text?
        ListOperationResult<?> result = null;
        try {
            if (isJSON) {
                result = listAsJSON(client, contentKey(partition, pathPrefix));
            } else {
                result = listAsText(client, contentKey(partition, pathPrefix));
            }
        } catch (Exception e) {
            System.err.println("ERROR: ("+e.getClass().getName()+"): "+e.getMessage());
            if (e instanceof RuntimeException) {
                e.printStackTrace(System.err);
            }
            System.exit(1);
        }
        client.stop();
        if (result.failed()) {
            System.err.println("Call failure when listing entries: "+result.getFirstFail().getFirstCallFailure());
            System.exit(2);
        }
    }

    private ListOperationResult<?> listAsJSON(BasicTSClient client, BasicTSKey prefix) throws InterruptedException
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
            if (left < 1) {
                return result;
            }
        }
    }

    private ListOperationResult<?> listAsText(BasicTSClient client, BasicTSKey prefix)
            throws InterruptedException, IOException
    {
        int left = Math.max(1, maxEntries);
        StoreEntryLister<BasicTSKey, ListItem> lister = client.listContent(prefix, ListItemType.entries);
        // we'll have to deserialize, serialize back...
        ObjectWriter w = jsonWriter(ListItemType.entries.getValueType());
        // let's wrap output in JSON array (or not?)
        JsonGenerator jgen = w.getJsonFactory().createGenerator(System.out);
        try {
            jgen.writeStartArray();
            while (true) {
                ListOperationResult<?>  result = lister.listMore(Math.min(left, 50));
                if (result.failed()) { // note: no closing JSON array for failures
                    return result;
                }
                List<?> items = result.getItems();
                if (items.isEmpty()) {
                    jgen.writeEndArray();
                    jgen.writeRaw("\n");
                    return result;
                }
                for (Object item : items) {
                    w.writeValue(jgen, item);
                    // should we force this or not... ?
                    jgen.writeRaw("\n");
                    --left;
                }
                if (left < 1) {
                    return result;
                }
            }
        } finally {
            jgen.close();
        }
    }
}
