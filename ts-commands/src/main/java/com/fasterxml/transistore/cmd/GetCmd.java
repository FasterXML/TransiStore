package com.fasterxml.transistore.cmd;

import java.io.*;
import java.util.*;

import com.fasterxml.jackson.core.JsonGenerator;

import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;

import com.fasterxml.storemate.shared.StorableKey;

import com.fasterxml.clustermate.api.ListItemType;
import com.fasterxml.clustermate.client.operation.ListOperationResult;
import com.fasterxml.clustermate.client.operation.StoreEntryLister;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.client.BasicTSClient;
import com.fasterxml.transistore.client.BasicTSClientConfig;

@Command(name = "get", description = "GET file(s) from TStore into local file system")
public class GetCmd extends TStoreCmdBase
{
    @Option(name = { "-m", "--max" }, description = "Maximum number of entries to get",
            arity=1 )
    public int maxEntries = Integer.MAX_VALUE;

    @Arguments(title="arguments",
            description = "Path prefix for entries to get (first argument), local directory to save them under (second)"
            ,usage="[server-prefix] [target directory]"
            ,required=true)
    public List<String> arguments;
    
    @Override
    public void run()
    {
        SkeletalServiceConfig serviceConfig = getServiceConfig();
        BasicTSClientConfig clientConfig = getClientConfig();

        if (arguments == null || arguments.size() != 2) {
            throw new IllegalArgumentException("Wrong number of arguments; expect two (server-prefix, target dir)");
        }
        BasicTSClient client = bootstrapClient(clientConfig, serviceConfig);

        // and then verify that all server sources are valid paths as well
        BasicTSKey prefix;
        try {
            prefix = contentKey(arguments.get(0));
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid server entry reference: "+e.getMessage());
        }
        
        // Let's actually verify that the second argument is an existing directory, to avoid
        // problems with missing/mixed arguments
        String dirName = arguments.get(1);
        File target = new File(dirName);
        if (!target.exists() || !target.isDirectory()) {
            throw new IllegalArgumentException("No directory with name '"+target.getAbsolutePath()+"'");
        }

        JsonGenerator jgen = null;        
        try {
            jgen = isJSON ? jsonGenerator(System.out) : null;
            if (jgen != null) {
                jgen.writeStartArray();
            }
            int fileCount = _getStuff(client, prefix, jgen, target);
            if (jgen == null) {
                System.out.printf("COMPLETE: downloaded %s files\n", fileCount);
            } else {
                jgen.writeEndArray();
                jgen.close();
            }
        } catch (Exception e) {
            if (jgen != null) {
                try { jgen.flush(); } catch (Exception e2) { }
            }
            System.err.printf("ERROR: (%s): %s", e.getClass().getName(), e.getMessage());
            if (e instanceof RuntimeException) {
                e.printStackTrace(System.err);
            }
            System.exit(1);
        }
        client.stop();
    }

    private int _getStuff(BasicTSClient client, BasicTSKey prefix, JsonGenerator jgen, File target)
        throws InterruptedException, IOException
    {
        String pathPrefix = prefix.getPath();
        String pathPrefix2 = null;
        
        if (pathPrefix == null || pathPrefix.length() == 0) { // basically, no prefix
            pathPrefix = "";
        } else if (pathPrefix.endsWith("/")) { // specifically contents of a logical dir
            ; // no change
        } else { // could be dir, or partial filename prefix
            int ix = pathPrefix.lastIndexOf('/');
            // and so we have to consider both possibilities; i.e. shorter version
            // for siblings, and full version with appended slash for children
            pathPrefix2 = pathPrefix.substring(0, ix+1);
            pathPrefix = pathPrefix + "/";
        }
        
        int left = Math.max(1, maxEntries);
        StoreEntryLister<BasicTSKey, StorableKey> lister = client.listContent(prefix, ListItemType.ids);
        int gotten = 0;

        while (true) {
            ListOperationResult<StorableKey> result = lister.listMore(Math.min(left, 100));
            if (result.failed()) {
                throw new IllegalArgumentException("Call failure (after "+gotten+" entries) when listing entries: "+result.getFirstFail().getFirstCallFailure());
            }
            List<StorableKey> keys = result.getItems();
            for (StorableKey rawKey : keys) {
                --left;
                // And then figure out part of server-side path to use for local entries
                BasicTSKey key = contentKey(rawKey);
                String entryPath = key.getPath();
                if (entryPath.startsWith(pathPrefix)) {
                    entryPath = entryPath.substring(pathPrefix.length());
                } else if ((pathPrefix2 != null) && entryPath.startsWith(pathPrefix2)) {
                    entryPath = entryPath.substring(pathPrefix2.length());
                } else {
                    throw new IllegalArgumentException("Received path that does not start with prefix ('"
                            +prefix.getPath()+"'): '"+key.getPath()+"'");
                }
                File dst = appendPath(target, entryPath);
                // also: need to ensure path exists, so
                File parent = dst.getParentFile();
                if (!parent.exists()) {
                    if (!parent.mkdirs()) {
                        warn("failed to created directory '%s'", parent.getAbsolutePath());
                    }
                    // probably fails, but let it go...
                }
                
                // And then just download it
                File resp = client.getContentAsFile(key, dst);
                if (jgen != null) {
                    writeJsonEntry(key, dst, jgen);
                } else {
                    if (resp == null) {
                        warn("no content for '%s' (concurrent DELETE?)\n", KEY_CONVERTER.keyToString(key));
                    } else {
                        System.out.printf("GOT entry '%s' as file '%s'\n", KEY_CONVERTER.keyToString(key),
                                dst.getAbsolutePath());
                    }
                }
            }
            if (left < 1 || keys.isEmpty()) {
                return gotten;
            }
            ++gotten;
        }
    }

    private void writeJsonEntry(BasicTSKey key, File resultFile, JsonGenerator jgen) throws IOException
    {
        jgen.writeStartObject();
        jgen.writeStringField("entry", KEY_CONVERTER.keyToString(key));
        if (resultFile == null) {
            jgen.writeNullField("file");
        } else {
            jgen.writeStringField("file", resultFile.getAbsolutePath());
        }
        jgen.writeEndObject();
    }
}
