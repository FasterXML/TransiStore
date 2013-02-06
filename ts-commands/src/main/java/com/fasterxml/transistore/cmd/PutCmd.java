package com.fasterxml.transistore.cmd;

import java.io.*;
import java.util.*;

import com.fasterxml.jackson.core.JsonGenerator;

import com.fasterxml.clustermate.client.NodeFailure;
import com.fasterxml.clustermate.client.operation.PutOperationResult;

import io.airlift.command.Arguments;
import io.airlift.command.Command;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.client.*;

@Command(name = "put", description = "PUT file(s) from local file system to TStore")
public class PutCmd extends TStoreCmdBase
{
    private final static long SMALL_FILE = 8000;
    
    @Arguments(title="arguments",
            description = "Target (first argument) and File(s) and/or directories to copy (recursively) (remaining)",
            usage="[server-prefix] [file-or-dir1] ... [file-or-dir-N]",
            required=true)
    public List<String> arguments;

    protected BasicTSKey _target;
    
    @Override
    public void run()
    {
        SkeletalServiceConfig serviceConfig = getServiceConfig();
        BasicTSClientConfig clientConfig = getClientConfig();

        if (arguments == null || arguments.size() < 2) {
            throw new IllegalArgumentException("Nothing to PUT");
        }
        BasicTSClient client = bootstrapClient(clientConfig, serviceConfig);
        // Ok, first, target, verify it is valid TStore path
        try {
            _target = contentKey(arguments.get(0));
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid prefix '"+arguments.get(0)+"', problem: "+e.getMessage());
        }
        // similarly verify that files/directories actually exist first
        List<File> input = new ArrayList<File>();
        for (int i = 1; i < arguments.size(); ++i) {
            File f = new File(arguments.get(i));
            if (!f.exists()) {
                throw new IllegalArgumentException("No file or directory '"+f.getAbsolutePath()+"'");
            }
            input.add(f);
        }

        JsonGenerator jgen = null;        
        try {
            jgen = isJSON ? jsonGenerator(System.out) : null;
            if (jgen != null) {
                jgen.writeStartArray();
            }
            int fileCount = _putStuff(client, input, jgen);
            if (jgen == null) {
                System.out.printf("COMPLETE: uploaded %s files\n", fileCount);
            } else {
                jgen.writeEndArray();
                jgen.close();
            }
        } catch (Exception e) {
            if (jgen != null) {
                try { jgen.flush(); } catch (Exception e2) { }
            }
            System.err.println("ERROR: ("+e.getClass().getName()+"): "+e.getMessage());
            if (e instanceof RuntimeException) {
                e.printStackTrace(System.err);
            }
            System.exit(1);
        }
        client.stop();
    }

    private int _putStuff(BasicTSClient client, List<File> input, JsonGenerator jgen)
        throws InterruptedException, IOException
    {
        int count = 0;
        
        for (File file : input) {
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
        String serverPrefix = _target.getPath();
        String local = pathFromFile(src);
        String path;
        
        if (serverPrefix == null) {
            path = local;
        } else {
            if (serverPrefix.endsWith("/")) { // local path always starts with slash, trim away
                path = serverPrefix + local.substring(1);
            } else {
                path = serverPrefix + local;
            }
        }
        return contentKey(_target.getPartitionId(), path); 
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
