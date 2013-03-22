package com.fasterxml.transistore.cmd;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.fasterxml.clustermate.client.operation.PutOperationResult;
import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.client.BasicTSClient;
import com.fasterxml.transistore.client.BasicTSClientConfig;

import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;

@Command(name = "generate", description = "Generate and upload test data in TStore")
public class GenerateLoad extends TStoreCmdBase
{
    /**
     * Number of milliseconds between creating new threads during startup
     */
    protected final static long THREAD_STARTUP_DELAY_MSECS = 1000L;

    @Option(name = { "-t", "--threads" }, description = "Number of threads to use (per operation type)")
    public int threadCount = 50;

    @Option(name = { "-c", "--count" }, description = "Number of requests to send (per operation type)")
    public int requestCount = 10000;
    
    @Option(name = { "-s", "--entrySize" }, description = "Size of entries to PUT")
    public long requestSize = 30000;
    
    @Arguments(title="server-path",
           description = "Path prefix to use for generated entries"
           ,usage="[server-prefix]"
           ,required=true)
    public List<String> arguments;

    protected BasicTSClient _client;

    protected BasicTSKey _prefix;
    
    public GenerateLoad() {
       // true -> Ok to write verbose info on stdout
        super(true);
    }

    @Override
    public void run()
    {
        SkeletalServiceConfig serviceConfig = getServiceConfig();
        BasicTSClientConfig clientConfig = getClientConfig();
        final long startTime = System.currentTimeMillis();

        if (arguments == null || arguments.size() != 1) {
            throw new IllegalArgumentException("Wrong number of arguments; expect one (server-prefix)");
        }
        _client = bootstrapClient(clientConfig, serviceConfig);

        // and then verify that all server sources are valid paths as well
        try {
            _prefix = contentKey(arguments.get(0));
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid server entry reference: "+e.getMessage());
        }

        final ExecutorService exec = Executors.newFixedThreadPool(threadCount);
        final ContentGenerator gen = new ContentGenerator(requestCount, requestSize);
        for (int i = 0; i < threadCount; ++i) {
            exec.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (!_performOperation(gen)) {
                            
                        }
                    } catch (Exception e) {
                        System.err.println("ERROR: "+e.getMessage());
                    }
                }
            });
        }
        double taken =  (System.currentTimeMillis() - startTime) / 1000.0;
        System.out.printf("DONE: sent %d requests in %.1f seconds\n", requestCount, taken);
    }

    protected boolean _performOperation(ContentGenerator gen)
            throws Exception
    {
        int index = gen.nextIndex();
        if (index < 0) {
            System.out.println("Thread completed");
            return false;
        }
        byte[] stuff = gen.generateContent(index);
        long startTime = System.nanoTime();
        BasicTSKey entryKey = contentKey(_prefix.getPartitionId(),
                _prefix.getPath() + "/entry_"+Integer.toHexString(index));
        PutOperationResult result = _client.putContent(entryKey, stuff);
        int msecs = (int) ((System.nanoTime() - startTime) >> 20); // about right
        _logPut(result, msecs, entryKey.toString());
        return true;
    }

    protected void _logPut(PutOperationResult result, int msecs, String key)
    {
        String status;
        
        if (!result.succeededMinimally()) {
            status = "F";
        } else {
            status = String.valueOf(result.getSuccessCount());
        }
        String msg = String.format("PUT/%s/%03d/%s\n", status, msecs, key);
        synchronized (this) {
            System.out.print(msg);
        }
    }
    
    /*
    /**********************************************************************
    /* Helper classes
    /**********************************************************************
     */

    static class ContentGenerator
    {
        final static int DIFF_ENTRIES = 16;

        protected int _countLeft;

        protected final byte[][] _contents = new byte[DIFF_ENTRIES][];
        
        public ContentGenerator(int count, long size)
        {
            _countLeft = count;
            Random rnd = new Random(count);
            for (int i = 0; i < DIFF_ENTRIES; ++i) {
                rnd.setSeed(i);
                _contents[i] = _generate((int) size, rnd);
            }
        }

        static byte[] _generate(int size, Random rnd)
        {
            byte[] result = new byte[size];
            for (int i = 0; i < size; ++i) {
                int c = rnd.nextInt() & 0xFF;
                // make mildly more compressible; ctrls -> spaces
                if (c < 32) {
                    c = ' ';
                } else if (c >= 127) { // and non-ASCII as upper-case ascii letters
                    c = 64 + (c & 0x1F);
                }
                result[i] = (byte) c;
            }
            return result;
        }

        public synchronized int nextIndex() {
            if (_countLeft < 1) { // all done?
                return -1;
            }
            return --_countLeft;
        }

        public synchronized byte[] generateContent(int index)
        {
           return _contents[index & (DIFF_ENTRIES-1)];
        }
    }
}
