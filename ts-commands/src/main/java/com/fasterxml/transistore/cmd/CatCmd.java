package com.fasterxml.transistore.cmd;

import java.io.*;
import java.util.*;

import com.fasterxml.clustermate.client.call.GetContentProcessor;
import com.fasterxml.clustermate.client.operation.GetOperationResult;
import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.client.BasicTSClient;
import com.fasterxml.transistore.client.BasicTSClientConfig;

import io.airlift.command.*;

/**
 * Command that works similar to Unix "cat" command, displaying contents of one
 * or more stores, writing them to stdout.
 */
@Command(name = "cat", description = "GET file(s) from TStore into local file system")
public class CatCmd extends TStoreCmdBase
{
    @Arguments(title="paths",
            description = "Server path(s) for entry to show)"
            ,usage="[path1] ... [pathN]")
    public List<String> paths;
    
    public CatCmd() {
        // false -> not ok to print stuff to stdout
        super(false);
    }
    
    @Override
    public void run()
    {
        SkeletalServiceConfig serviceConfig = getServiceConfig();
        BasicTSClientConfig clientConfig = getClientConfig();

        if (paths == null || paths.size() == 0) { // or could read from stdin?
            throw new IllegalArgumentException("No entries to display");
        }
        BasicTSClient client = bootstrapClient(clientConfig, serviceConfig);

        // and then verify that all paths are valid
        List<BasicTSKey> pathList = new ArrayList<BasicTSKey>(paths.size());
        for (String path : paths) {
            try {
                pathList.add(contentKey(path));
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid path '"+path+"': "+e.getMessage());
            }
        }

        final OutputStream out = System.out;
        for (BasicTSKey path : pathList) {
            GetOperationResult<Long> resp = null;
            try {
                resp = client.getContent(null, path,
                        new GetContentProcessor<Long>() {
                            @Override
                            public GetContentProcessor.Handler<Long> createHandler() {
                                return new GetContentProcessor.Handler<Long>() {
                                    long bytes = 0L;

                                    @Override
                                    public boolean processContent(byte[] content, int offset, int length)
                                            throws IOException {
                                        out.write(content, offset, length);
                                        bytes += length;
                                        return true;
                                    }

                                    @Override
                                    public Long completeContentProcessing() { return bytes; }

                                    @Override
                                    public void contentProcessingFailed(Throwable t) { }
                                };
                            }
                });
            } catch (Exception e) {
                terminateWith(e);
            } finally {
                try {
                    out.flush();
                } catch (IOException e) { }
            }
            // Failure to contact nodes? Abort
            if (resp.failed()) {
                throw new IllegalStateException("Failed to display entry '"+path+"': "+resp.getFailCount()
                        +" failed nodes tried -- first error: "+resp.getFirstFail());
            }
            // Didn't find anything? Whine but continue
            if (!resp.entryFound()) {
                warn("Entry '%s' not found",  path);
            }
        }
        
        client.stop();
    }
}
