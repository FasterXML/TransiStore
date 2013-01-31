package com.fasterxml.transistore.cmd;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.client.*;
import com.fasterxml.clustermate.client.NodeFailure;
import com.fasterxml.clustermate.client.operation.PutOperationResult;
import com.fasterxml.jackson.core.JsonGenerator;

import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;

@Command(name = "upload", description = "Upload files from local file system to TStore")
public class UploadCmd extends TStoreCmdBase
{
    final static long SMALL_FILE = 8000;

    // Let's keep partition required, for now. Empty String is acceptable
    @Option(name = { "-p", "--partition" },
            description = "Server-side prefix to use, if any; if defined, uses relative names, if not, absolute",
            required = true)
    public String partition;

    @Option(name = { "-s", "--server-prefix" },
            description = "Server-side prefix to use, if any; if defined, uses relative names, if not, absolute")
    public String serverPrefix;
    
    @Arguments(description = "Files and/or directories to copy (recursively)",
            required=true)
    public List<String> source;
    
    @Override
    public void run()
    {
        SkeletalServiceConfig serviceConfig = getServiceConfig();
        BasicTSClientConfig clientConfig = getClientConfig();

        if (source == null || source.isEmpty()) { // at least one source thingy
            throw new IllegalArgumentException("Nothing to upload");
        }
        BasicTSClient client = bootstrapClient(clientConfig, serviceConfig);
        try {
            JsonGenerator jgen = isJSON ? jsonGenerator(System.out) : null;
            if (jgen != null) {
                jgen.writeStartArray();
            }
            int fileCount = _copyStuff(client, source, jgen);
            if (jgen == null) {
                System.out.printf("COMPLETE: uploaded %s files\n", fileCount);
            } else {
                jgen.writeEndArray();
            }
        } catch (Exception e) {
            System.err.println("ERROR: ("+e.getClass().getName()+"): "+e.getMessage());
            if (e instanceof RuntimeException) {
                e.printStackTrace(System.err);
            }
            System.exit(1);
        }
        client.stop();
    }

    private int _copyStuff(BasicTSClient client, List<String> input, JsonGenerator jgen)
        throws InterruptedException, IOException
    {
        int count = 0;
        
        for (String filename : input) {
            File file = new File(filename);
            System.err.println(" File -> "+file);
            count += _copyFileOrDir(client, file, jgen);
        }
        return count;
    }

    private int _copyFileOrDir(BasicTSClient client, File src, JsonGenerator jgen)
            throws InterruptedException, IOException
    {
        if (src.isDirectory()) {
            if (isTextual) {
                System.out.printf("Copy directory '%s':\n", src.getAbsolutePath());
            }
            int count = 0;
            for (File f : src.listFiles()) {
                count += _copyFileOrDir(client, f, jgen);
            }
            if (isTextual) {
                System.out.printf("-> directory '%s' complete with %d files.\n",
                        src.getAbsolutePath(), count);
            }
            return count;
        }
        return _copyFile(client, src, jgen);
    }
    
    private int _copyFile(BasicTSClient client, File src, JsonGenerator jgen)
            throws InterruptedException, IOException
    {
        // use byte-backed for some, just to get better testing...
        boolean isSmall = (src.length() < SMALL_FILE);
        
        BasicTSKey key = keyFor(src);
        
        PutOperationResult putResult = isSmall ? client.putContent(key, readFile(src))
                : client.putContent(key, src);
        
        if (!putResult.succeededMinimally()) {
            NodeFailure fail = putResult.getFirstFail();
            int status = fail.getFirstCallFailure().getStatusCode();
            if (status == 409) {
                System.err.printf("WARN: Conflict (409) for server entry '%s'; skipping\n", key);
                return 0;
            }
            throw new IOException("Failed to PUT copy of '"+key+"': "+putResult.getFailCount()
                    +" failed nodes tried -- first error: "+putResult.getFirstFail());
        }
        int copies = putResult.getSuccessCount();
        if (!putResult.succeededOptimally()) {
            NodeFailure fail = putResult.getFirstFail();
            if (fail == null) {
                System.err.printf("(WARN: sub-optimal PUT, %d copies -- no FAIL info?)\n", copies);
            } else {
                System.err.printf("(WARN: sub-optimal PUT, %d copies; %s failed, first fail status: %d)\n",
                        copies, fail.getServer().getAddress(), fail.getFirstCallFailure().getStatusCode());
            }
        }
        if (jgen != null) {
            jgen.writeString(key.toString());
            jgen.writeRaw('\n');
        } else {
            if (verbose) {
                System.out.printf("Uploaded file '%s' as '%s'\n", src.getPath(), key);
            }
        }
        return 1;
    }

    private BasicTSKey keyFor(File src)
    {
        // Use relative name, if we got prefix; absolute if not
        String path = (serverPrefix == null) ? src.getAbsolutePath() :
            (serverPrefix + src.getPath());
        return contentKey(partition, path); 
    }

    private static byte[] readFile(File f) throws IOException
    {
        final int len = (int) f.length();
        byte[] result = new byte[len];
        int offset = 0;

        FileInputStream in = new FileInputStream(f);
        try {
            while (offset < len) {
                int count = in.read(result, offset, len-offset);
                if (count <= 0) {
                    throw new IOException("Failed to read file '"+f.getAbsolutePath()+"'; needed "+len+" bytes, got "+offset);
                }
                offset += count;
            }
        } finally {
            try { in.close(); } catch (IOException e) { }
        }
        return result;
    }
}
