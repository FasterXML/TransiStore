package com.fasterxml.transistore.cmd;

import java.io.*;
import java.util.*;

import com.fasterxml.clustermate.client.call.GetContentProcessor;
import com.fasterxml.clustermate.client.operation.GetOperationResult;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.client.BasicTSClient;

import io.airlift.command.*;

/**
 * Command that works similar to Unix "cat" command, displaying contents of one
 * or more stores, writing them to stdout.
 */
@Command(name = "cat", description = "GET file(s) from TStore into local file system")
public class CatCmd extends TStoreCmdBase
{
    @Arguments(title="paths",
            description = "Server path(s) for entry to show"
            ,usage="[path1] ... [pathN]")
    public List<String> paths;
    
    public CatCmd() {
        // false -> not ok to print stuff to stdout
        super(false);
    }
    
    @Override
    public void run()
    {
        if (paths == null || paths.size() == 0) { // or could read from stdin?
            throw new IllegalArgumentException("No entries to display");
        }
        BasicTSClient client = bootstrapClient();

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
        final long start = System.nanoTime();
        boolean fail = false;
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
                                    public boolean startContent(int statusCode, Compression compression) {
                                        // anything we should take into account here?
                                        return true;
                                    }
                                    
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
                System.err.printf("Failed to display entry '%s': %d failed nodes tried -- first error: %s",
                        path, resp.getFailCount(), resp.getFirstFail());
                fail = true;
                break;
            }
            // Didn't find anything? Whine but continue
            if (!resp.entryFound()) {
                warn("Entry '%s' not found",  path);
            }
        }
        if (verbose) {
            double millis = (System.nanoTime() - start) / (1000.0 * 1000.0);
            System.out.println();
            System.out.printf("DEBUG: took %.1f msec", millis);
            System.out.println();
        }
        
        client.stop();
        if (fail) {
            System.exit(1);
        }
    }
}
