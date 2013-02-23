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

    private final static String STDIN_MARKER = "-";
    
    private final static File STDIN_MARKER_FILE = new File(STDIN_MARKER);
    
    @Arguments(title="arguments",
            description = "Target (first argument) and File(s) and/or directories to copy recursively (remaining arguments)."
            +" Note that '"+STDIN_MARKER+"' can be used to mean stdin (but only as only source)",
            usage="[server-prefix] [file-or-dir1] ... [file-or-dir-N]",
            required=true)
    public List<String> arguments;

    protected BasicTSKey _target;
    
    public PutCmd() {
        // true -> Ok to write verbose info on stdout
        super(true);
    }

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
        // Special case: "-" as the only source?
        if (arguments.size() == 2 && STDIN_MARKER.equals(arguments.get(1))) {
            input.add(STDIN_MARKER_FILE);
        } else {
            for (int i = 1; i < arguments.size(); ++i) {
                File f = new File(arguments.get(i));
                if (!f.exists()) {
                    throw new IllegalArgumentException("No file or directory '"+f.getAbsolutePath()+"'");
                }
                input.add(f);
            }
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
            if (file == STDIN_MARKER_FILE) {
                _copyStdIn(client, file, jgen);
                ++count;
            } else {
                count += _copyFileOrDir(client, file, new File(file.getName()), jgen);
            }
        }
        return count;
    }

    private void _copyStdIn(BasicTSClient client, File dst, JsonGenerator jgen)
            throws InterruptedException, IOException
    {
        // Let's actually copy stuff from stdin into a temporary file, upload that
        File tmpFile = File.createTempFile("tstore", ".tmp");
        tmpFile.deleteOnExit();
        byte[] buffer = new byte[4000];
        InputStream in = System.in;
        FileOutputStream out = new FileOutputStream(tmpFile);

        int count;
        while ((count = in.read(buffer)) > 0) {
            out.write(buffer, 0, count);
        }
        out.close();
        _copyFile(client, tmpFile, dst, jgen);
    }
    
    private int _copyFileOrDir(BasicTSClient client, File src, File dst, JsonGenerator jgen)
            throws InterruptedException, IOException
    {
        if (src.isDirectory()) {
            if (isTextual) {
                System.out.printf("Copy directory '%s':\n", src.getAbsolutePath());
            }
            int count = 0;
            for (File f : src.listFiles()) {
                File dir = new File(dst, f.getName());
                count += _copyFileOrDir(client, f, dir, jgen);
            }
            if (isTextual) {
                System.out.printf("-> directory '%s' complete with %d files.\n",
                        src.getAbsolutePath(), count);
            }
            return count;
        }
        return _copyFile(client, src, dst, jgen);
    }
    
    private int _copyFile(BasicTSClient client, File src, File dst, JsonGenerator jgen)
            throws InterruptedException, IOException
    {
        // use byte-backed for some, just to get better testing...
        boolean isSmall = (src.length() < SMALL_FILE);
        
        BasicTSKey key = (dst == STDIN_MARKER_FILE) ? _target : keyFor(dst);
        
        final long nanoStart = System.nanoTime();
        
        PutOperationResult putResult = isSmall ? client.putContent(key, readFile(src))
                : client.putContent(key, src);
        if (!putResult.succeededMinimally()) {
            NodeFailure fail = putResult.getFirstFail();
            int status = fail.getFirstCallFailure().getStatusCode();
            if (status == 409) {
                warn("Conflict (409) for server entry '%s'; skipping\n", key);
                return 0;
            }
            throw new IOException("Failed to PUT copy of '"+key+"': "+putResult.getFailCount()
                    +" failed nodes tried -- first error: "+putResult.getFirstFail());
        }
        int copies = putResult.getSuccessCount();
        if (!putResult.succeededOptimally()) {
            NodeFailure fail = putResult.getFirstFail();
            if (fail == null) {
                warn("(sub-optimal PUT, %d copies -- no FAIL info?)\n", copies);
            } else {
                warn("(sub-optimal PUT, %d copies; %s failed, first fail status: %d)\n",
                        copies, fail.getServer().getAddress(), fail.getFirstCallFailure().getStatusCode());
            }
        }
        if (jgen != null) {
            jgen.writeString(key.toString());
            jgen.writeRaw('\n');
        } else {
            if (verbose) {
                int msecs = (int) ((System.nanoTime() - nanoStart) >> 20);
                System.out.printf("Uploaded file '%s' as '%s'\n (in %d msecs)", src.getPath(), key, msecs);
            }
        }
        return 1;
    }

    private BasicTSKey keyFor(File src)
    {
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
