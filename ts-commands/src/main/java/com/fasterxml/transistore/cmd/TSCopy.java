package com.fasterxml.transistore.cmd;

import java.io.*;

import com.fasterxml.clustermate.client.NodeFailure;
import com.fasterxml.clustermate.client.operation.DeleteOperationResult;
import com.fasterxml.clustermate.client.operation.PutOperationResult;
import com.fasterxml.clustermate.json.ClusterMateObjectMapper;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.basic.BasicTSKeyConverter;
import com.fasterxml.transistore.client.*;
import com.fasterxml.transistore.client.ahc.AHCBasedClientBootstrapper;

public class TSCopy
{
    /**
     * We'll use a threshold to exercise both file- and byte-backed variants;
     * exact value does not matter a lot, use 4k for fun.
     */
    private final static long SMALL_FILE = 4000L;
    
    private BasicTSClientConfig _clientConfig = BasicTSClientConfig.defaultConfig();

    protected int _totalFiles;
    protected long _totalBytes;
    
    protected boolean _doDelete;

    /*
    ///////////////////////////////////////////////////////////////////////
    // Construction
    ///////////////////////////////////////////////////////////////////////
     */
    
    public TSCopy() { }

    public static void main(String[] args) throws Exception {
        new TSCopy().run(args);
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Copy implementation
    ///////////////////////////////////////////////////////////////////////
     */
    
    private void showUsage()
    {
        System.err.println("Usage: java "+getClass().getName()+" [-d] <config-file> <src-dir> <dst-dir>");
        System.exit(1);
    }
    
    public void run(String[] args) throws InterruptedException, IOException
    {
        if (args.length < 3 || args.length > 4) {
            System.err.printf("Got %d arguments, need 3.\n", args.length);
            showUsage();
        }
        _doDelete = false;
        if (args.length == 4) { // -d switch?
            if ("-d".equals(args[1])) {
                _doDelete = true;
            } else { // clumsy...
                System.err.printf("Got 4 arguments, but second not a known switch (-d)\n", args.length);
                System.err.println("Usage: java "+getClass().getName()+" [config-file] [src-dir] [dst-dir]");
                System.exit(1);
            }
        }
        
        File configFile = new File(String.valueOf(args[0]));
        int offset = _doDelete ? 1 : 0;
        File src = new File(String.valueOf(args[offset+1]));
        File dst = new File(String.valueOf(args[offset+2]));
        if (!configFile.isFile()) {
            System.err.println("Config file does not exist: "+configFile.getAbsolutePath());
            System.exit(1);
        }
        if (!src.isDirectory()) {
            System.err.println("Source directory does not exist: "+src.getAbsolutePath());
            System.exit(1);
        }
        if (dst.exists() && !dst.isDirectory()) {
            System.err.println("Destination exists but is not a directory: "+dst.getAbsolutePath());
            System.exit(1);
        }
        final ObjectMapper mapper = new ClusterMateObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        final ObjectReader reader = mapper.reader(SkeletalServiceConfig.class)
                .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        final SkeletalServiceConfig config = reader.readValue(configFile);

        System.out.print("Configuring StoreClient:");
        // assume we typically run for real cluster:
        int optimalNodes = 2;
        int maxNodes = 3;

        _clientConfig = new BasicTSClientConfigBuilder()
                .setOptimalOks(optimalNodes)
                .setMaxOks(maxNodes)
                .build();
        BasicTSClientBootstrapper bs = new AHCBasedClientBootstrapper(_clientConfig);
        int count = 0;
        for (SkeletalServiceConfig.Node node : config.v.cluster.clusterNodes) {
            System.out.print(" "+node.ipAndPort);
            bs = bs.addNode(node.ipAndPort);
            ++count;
        }
        System.out.printf(" (%d nodes)\n", count);
        System.out.print("bootstrapping the client (wait up to 5 seconds): ");

        BasicTSClient client = null;
        long start = System.currentTimeMillis();
        long msecs = 0L;
        try {
            client = bs.buildAndInitCompletely(5);

            System.out.println("Client initialized -- let's start copying (delete? "+_doDelete+")!");
            _totalFiles = 0;
            _totalBytes = 0;

            start = System.currentTimeMillis();
            _copyDirectory(client, src, dst);
        } finally {
            msecs = System.currentTimeMillis() - start;
            if (client != null) {
                client.stop();
            }
        }
        System.out.printf("DONE: Copied %d files, %d kB, in %.2f seconds\n",
                _totalFiles, _totalBytes>>10, msecs/1000.0);
    }

    private int _copyDirectory(BasicTSClient client, File srcDir, File dstDir)
            throws InterruptedException, IOException
    {
        // first things first: may need to create destination dir
        if (!dstDir.isDirectory()) {
            dstDir.mkdirs();
        }
        
        int files = 0;
        for (File src : srcDir.listFiles()) {
            String name = src.getName();
            File dst = new File(dstDir, name);
            if (src.isDirectory()) {
                System.out.printf("Copy directory '%s':\n", src.getAbsolutePath());
                int count = _copyDirectory(client, src, dst);
                System.out.printf("-> directory '%s' complete with %d files.\n",
                        srcDir.getAbsolutePath(), count);
            } else {
                final int size = (int) src.length();
                System.out.printf("  copy file '%s' (%d kB): ", name, size>>10);
                long now = System.currentTimeMillis();
                int copies = _copyFile(client, src, dst);
                long msecs = System.currentTimeMillis() - now;
                System.out.printf("%d copies stored in %d msecs\n", copies, msecs);
                ++files;
                ++_totalFiles;
                _totalBytes += size;
            }
        }
        return files;
    }

    private int _copyFile(BasicTSClient client, File src, File dst)
            throws InterruptedException, IOException
    {
        /* one tweak: use byte-backed one for smaller files; not because of optimization
         * but more to try out different combinations for testing purposes.
         */
        boolean isSmall = (src.length() < SMALL_FILE);
        BasicTSKeyConverter conv = (BasicTSKeyConverter) client.getKeyConverter();
        
        BasicTSKey key = conv.construct(src.getAbsolutePath());
        PutOperationResult putResult = isSmall ? client.putContent(_clientConfig, key, readFile(src))
                : client.putContent(_clientConfig, key, src);
        
        if (!putResult.succeededMinimally()) {
            throw new IOException("Failed to PUT copy of '"+key+"': "+putResult.getFailCount()
                    +" failed nodes tried -- first error: "+putResult.getFirstFail());
        }
        int copies = putResult.getSuccessCount();
        if (!putResult.succeededOptimally()) {
            NodeFailure fail = putResult.getFirstFail();
            if (fail == null) {
                System.out.printf("(WARN: sub-optimal PUT, %d copies -- no FAIL info?)", copies);
            } else {
                System.out.printf("(WARN: sub-optimal PUT, %d copies; %s failed)",
                        copies, fail.getServer().getAddress());
//System.out.printf(" (fail -> %s)", fail);
            }
        }
        // then d/l, write
        boolean ok;
        if (isSmall) {
            byte[] bytes = client.getContentAsBytes(_clientConfig, key);
            ok = (bytes != null);
            if (ok) {
                writeFile(dst, bytes);
            }
        } else {
            File f = client.getContentAsFile(_clientConfig, key, dst);
            ok = (f != null);
        }
        if (!ok) {
            throw new IOException("Failed to GET copy of '"+key+"' (small? "+isSmall+")");
        }
        // and finally DELETE?
        if (_doDelete) {
            DeleteOperationResult deleteResult = client.deleteContent(_clientConfig, key);
            if (!deleteResult.succeededMinimally()) {
                throw new IOException("Failed to DELETE copies of '"+key+"': "+deleteResult.getFailCount()+" failed nodes tried");
            }
            if (!deleteResult.succeededOptimally()) {
                System.out.printf("(WARN: sub-optimal DELETE, %d copies)", deleteResult.getSuccessCount());
            }
        } else {
            System.out.print("(DELETE disabled)");
        }
        // One more twist: verify file to make sure we copy things appropriately...
        int diffByte = compareFiles(src, dst);
        if (diffByte < 0) { // fine!
            System.out.print("[verified: OK] ");
        } else {
            throw new IOException("Copy failed for '"+src.getAbsolutePath()+"': contents differ! (src size: "
                    +src.length()+", dst size: "+dst.length()+")");
        }
        
        return copies;
    }

    private static int compareFiles(File src, File dst) throws IOException
    {
        int offset = 0;
        FileInputStream in1 = new FileInputStream(src);
        FileInputStream in2 = new FileInputStream(dst);
        byte[] buffer1 = new byte[4000];
        byte[] buffer2 = new byte[4000];
        int count1, count2;

        try {
            while (true) {
                // mildly incorrect: in theory, could read less data for either one but...
                count1 = in1.read(buffer1);
                count2 = in2.read(buffer2);
    
                final int end = Math.min(count1, count2);
                for (int i = 0; i < end; ++i) {
                    if (buffer1[i] != buffer2[i]) {
                        return (offset + i);
                    }
                }
                // also, if we read less, assume we got EOF for one so:
                if (count1 != count2) {
                    return offset + end;
                }
                if (count1 < 0) {
                    return -1;
                }
                offset += count1;
            }
        } finally {
            in1.close();
            in2.close();
        }
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // I/O helper methods
    ///////////////////////////////////////////////////////////////////////
     */

    private static void writeFile(File file, byte[] data) throws IOException {
        writeFile(file, data, 0, data.length);
    }
    
    private static void writeFile(File file, byte[] data, int offset, int length) throws IOException
    {
        FileOutputStream out = new FileOutputStream(file);
        out.write(data, offset, length);
        out.close();
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
