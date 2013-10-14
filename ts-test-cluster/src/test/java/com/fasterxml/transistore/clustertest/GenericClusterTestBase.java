package com.fasterxml.transistore.clustertest;

import java.io.*;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.skife.config.TimeSpan;

import ch.qos.logback.classic.Level;

import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.compress.Compressors;
import com.fasterxml.storemate.shared.hash.BlockMurmur3Hasher;
import com.fasterxml.storemate.shared.util.UTF8Encoder;
import com.fasterxml.storemate.store.AdminStorableStore;
import com.fasterxml.storemate.store.StoreException;
import com.fasterxml.storemate.store.StoreOperationSource;
import com.fasterxml.storemate.store.backend.StoreBackendConfig;

import com.fasterxml.clustermate.client.StoreClientBootstrapper;
import com.fasterxml.clustermate.service.cfg.ClusterConfig;
import com.fasterxml.clustermate.service.cfg.KeyRangeAllocationStrategy;
import com.fasterxml.clustermate.service.cfg.NodeConfig;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.basic.BasicTSKeyConverter;
import com.fasterxml.transistore.client.BasicTSClient;
import com.fasterxml.transistore.client.BasicTSClientConfig;
import com.fasterxml.transistore.clustertest.util.FakeHttpResponse;
import com.fasterxml.transistore.dw.BasicTSServiceConfigForDW;

import junit.framework.TestCase;

/**
 * Backend-independent part of tests, to be extended by concrete
 * implementations that plug specific StorableStore backend.
 */
public abstract class GenericClusterTestBase extends TestCase
{
    // null -> require client id with key
    protected final BasicTSKeyConverter _keyConverter = BasicTSKeyConverter.defaultInstance();

    /*
    /**********************************************************************
    /* Low-level StorableStore helpers
    /**********************************************************************
     */
    
    protected int calcChecksum(byte[] data) {
        return calcChecksum(data, 0, data.length);
    }

    protected int calcChecksum(byte[] data, int offset, int len ) {
        return BlockMurmur3Hasher.instance.hash(0, data, offset, len);
    }

    public StorableKey storableKey(String str) {
        return new StorableKey(UTF8Encoder.encodeAsUTF8(str));
    }
    
    /*
    /**********************************************************************
    /* Configuration setting helpers
    /**********************************************************************
     */

    protected BasicTSServiceConfigForDW createSimpleTestConfig(String testSuffix, boolean cleanUp)
        throws IOException
    {
        // Entry DB and file store settings:
        File testRoot = getTestScratchDir(testSuffix, cleanUp);
        BasicTSServiceConfigForDW config = new BasicTSServiceConfigForDW();
        File dbRoot = new File(testRoot, "v-test-entries");
        config.getServiceConfig().metadataDirectory = dbRoot;
        config.getServiceConfig().storeConfig.dataRootForFiles = new File(testRoot, "v-files");
        File storeDataDir = new File(testRoot, "v-store");
        config.getServiceConfig().overrideStoreBackendConfig(createBackendConfig(storeDataDir));
        return config;
    }

    protected abstract StoreBackendConfig createBackendConfig(File dataDir);
    
    protected BasicTSServiceConfigForDW createNodeConfig(String testSuffix,
            boolean cleanUp, int port, ClusterConfig cluster)
        throws IOException
    {
        BasicTSServiceConfigForDW config = createSimpleTestConfig(testSuffix, cleanUp);
        // Use different port than regular runs:
        config.overrideHttpPort(port);
        config.overrideAdminPort(port);
        // tone down logging
        config.getLoggingConfiguration().setLevel(Level.WARN);
        // specified cluster defs:
        config.getServiceConfig().cluster = cluster;
        // plus, important: reduce grace period (as we can use TimeMaster) to 1 millisecond:
        config.getServiceConfig().cfgSyncGracePeriod = new TimeSpan(1L, TimeUnit.MILLISECONDS);
        return config;
    }

    protected BasicTSServiceConfigForDW createSingleNodeConfig(String testSuffix,
            boolean cleanUp, int port)
        throws IOException
    {
        ClusterConfig cluster = new ClusterConfig();
        cluster.type = KeyRangeAllocationStrategy.STATIC;
        cluster.clusterKeyspaceSize = 360;
        ArrayList<NodeConfig> nodes = new ArrayList<NodeConfig>();
        nodes.add(new NodeConfig("localhost:"+port, 0, 360));
        // Cluster config? Set keyspace size, but nothing else yet
        cluster.clusterNodes = nodes;
        return createNodeConfig(testSuffix, cleanUp, port, cluster);
    }

    protected ClusterConfig twoNodeClusterConfig(IpAndPort endpoint1, IpAndPort endpoint2,
            int keyspaceLength)
    {
        ClusterConfig clusterConfig = new ClusterConfig();
        // Should be fine to use semi-dynamic allocation
        
        clusterConfig.clusterKeyspaceSize = keyspaceLength; // with 2 nodes, anything divisible by 2 is fine
        clusterConfig.numberOfCopies = 2;
        clusterConfig.type = KeyRangeAllocationStrategy.SIMPLE_LINEAR;
        clusterConfig.clusterNodes.add(new NodeConfig(endpoint1));
        clusterConfig.clusterNodes.add(new NodeConfig(endpoint2));
        return clusterConfig;
    }

    /*
    /**********************************************************************
    /* Other factory methods
    /**********************************************************************
     */

    protected BasicTSKey contentKey(String fullPath) {
        return _keyConverter.construct(fullPath);
    }

    protected BasicTSKey contentKey(String partition, String fullPath) {
        return _keyConverter.construct(partition, fullPath);
    }

    protected BasicTSKey contentKey(StorableKey raw) {
        return _keyConverter.rawToEntryKey(raw);
    }

    protected BasicTSClient createClient(BasicTSClientConfig clientConfig, IpAndPort... nodes)
        throws IOException
    {
        StoreClientBootstrapper<?,?,?,?> bs = createClientBootstrapper(clientConfig);      
        for (IpAndPort node : nodes) {
            bs = bs.addNode(node);
        }
        return (BasicTSClient) bs.buildAndInitCompletely(5);
    }

    protected abstract StoreClientBootstrapper<?,?,?,?> createClientBootstrapper(BasicTSClientConfig clientConfig);

    /*
    /**********************************************************************
    /* Service life cycle
    /**********************************************************************
     */
    
    protected void startServices(StoreForTests... services) throws Exception
    {
        for (StoreForTests service : services) {
            try {
                service.startTestService();
            } catch (java.net.BindException e) {
                fail("Failed to start test server due to bind exception: "+e.getMessage());
            }
        }
    }
    
    /*
    /**********************************************************************
    /* Methods for file, directory handling
    /**********************************************************************
     */
     
    /**
     * Method for accessing "scratch" directory used for tests.
     * We'll try to create this directory under 
     * Assumption is that the current directory at this point
     * is project directory.
     */
    protected File getTestScratchDir(String testSuffix, boolean cleanUp) throws IOException
    {
        File f = new File(new File("test-data"), testSuffix).getCanonicalFile();
        if (!f.exists()) {
            if (!f.mkdirs()) {
                throw new IOException("Failed to create test directory '"+f.getAbsolutePath()+"'");
            }
        } else if (cleanUp) {
            for (File kid : f.listFiles()) {
                deleteFileOrDir(kid);
            }
        }
        return f;
    }

    protected void deleteFileOrDir(File fileOrDir) throws IOException
    {
        if (fileOrDir.isDirectory()) {
            for (File kid : fileOrDir.listFiles()) {
                deleteFileOrDir(kid);
            }
        }
        if (!fileOrDir.delete()) {
            throw new IOException("Failed to delete test file/directory '"+fileOrDir.getAbsolutePath()+"'");
        }
    }

    protected byte[] readAll(File f) throws IOException
    {
        FileInputStream in = new FileInputStream(f);
        byte[] data = readAll(in);
        in.close();
        return data;
    }

    protected byte[] readAll(InputStream in) throws IOException
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(4000);
        byte[] buf = new byte[4000];
        int count;
        while ((count = in.read(buf)) > 0) {
            bytes.write(buf, 0, count);
        }
        in.close();
        return bytes.toByteArray();
    }

    protected byte[] collectOutput(FakeHttpResponse response) throws IOException
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(4000);
        response.getStreamingContent().writeContent(bytes);
        return bytes.toByteArray();
    }

    /*
    /**********************************************************************
    /* Test methods: data generation
    /**********************************************************************
     */
     
    protected String biggerCompressibleData(int size)
    {
        StringBuilder sb = new StringBuilder(size + 100);
        final Random rnd = new Random(123);
        while (sb.length() < size) {
            sb.append("Some data: ")
            .append(sb.length())
            .append("/")
            .append(sb.length())
            .append(rnd.nextInt()).append("\n");
        }
        return sb.toString();
    }

    protected byte[] biggerSomewhatCompressibleData(int size)
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(size+8);
        OutputStreamWriter w = new OutputStreamWriter(bytes);
        final Random rnd = new Random(123);
        try {
            while (bytes.size() < size) {
                int val = rnd.nextInt();
                switch (val % 5) {
                case 0:
                    w.write('X');
                    break;
                case 1:
                    w.write(": ");
                    w.write(bytes.size());
                    break;
                case 2:
                    w.write('\n');
                    break;
                case 3:
                    w.write((char) (33 + val & 0x3f));
                    break;
                default:
                    w.write("/");
                    w.write(Integer.toHexString(bytes.size()));
                    break;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return bytes.toByteArray();
    }
    
    protected String biggerRandomData(int size)
    {
        StringBuilder sb = new StringBuilder(size + 100);
        Random rnd = new Random(size);
        for (int i = 0; i < size; ++i) {
            sb.append((byte) (32 + rnd.nextInt() % 95));
        }
        return sb.toString();
    }

    protected byte[] lzfCompress(byte[] data) throws IOException {
        return Compressors.lzfCompress(data);
    }
    
    protected int rawHash(byte[] data)
    {
        return BlockMurmur3Hasher.instance.hash(data);
    }

    /*
    /**********************************************************************
    /* Test methods: message validation
    /**********************************************************************
     */

    protected void verifyException(Exception e, String expected)
    {
        verifyMessage(expected, e.getMessage());
    }
    
    protected void verifyMessage(String expectedPiece, String actual)
    {
        if (actual == null || actual.toLowerCase().indexOf(expectedPiece.toLowerCase()) < 0) {
            fail("Expected message that contains phrase '"+expectedPiece+"'; instead got: '"
                    +actual+"'");
        }
    }

    protected void verifyHash(String msg, int expHash, byte[] data) {
        verifyHash(msg, expHash, BlockMurmur3Hasher.instance.hash(data));
     }

    protected void verifyHash(String msg, int expHash, int actualHash)
    {
        if (expHash != actualHash) {
            assertEquals(msg, Integer.toHexString(expHash), Integer.toHexString(actualHash));
        }
    }
    
    /*
    /**********************************************************************
    /* Log setup, handling
    /**********************************************************************
     */

    /**
     * Method to be called before tests, to ensure logging does not whine.
     */
    protected void initTestLogging()
    {
        /*
        final org.slf4j.Logger logger = client.getLogger();
        if (!(logger instanceof ch.qos.logback.classic.Logger)) {
            System.err.println("WARN: cannot reconfigure test logging for Logger of type "+logger.getClass());
            return;
        }
        ch.qos.logback.classic.Logger logbackLogger = (ch.qos.logback.classic.Logger) logger;
        logbackLogger.setLevel(ch.qos.logback.classic.Level.WARN);
//        Log.named(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(Level.WARN);
        // Parts of system use j.u.l:
         */
    }

    /**
     * Helper method used to print out warning related to running of tests.
     * This does not necessarily use regular logger set up, as the purpose
     * is to distinguish these from logging done by system itself.
     */
    protected void testWarn(String msgFormat, Object...args)
    {
        String msg = String.format(msgFormat, args);
        System.err.println("TEST-WARN("+getClass().getName()+"): "+msg);
    }

    /*
    /**********************************************************************
    /* Cluster state verification
    /**********************************************************************
     */

    protected int expectState(String EXP, String msg, int expRounds, int maxRounds,
            StoreForTests store1, StoreForTests... stores)
                    throws InterruptedException, StoreException
    {
        int round = 1;
        while (true) {
            Thread.sleep(25L);
            store1.getTimeMaster().advanceCurrentTimeMillis(90000L);
            String act = storeCounts(store1, stores);
            if (act.equals(EXP)) {
                if (round > expRounds) { // one more thing: WARN if it took longer than expected
                    testWarn("expectState (for '%s') took %d rounds; expected at most %d",
                            msg, round, expRounds);
                }
                return round;
            }
            if (++round > maxRounds) {
                fail("Failed to change state to '"+EXP+"' in "+maxRounds+" rounds (got: "+act+"): "+msg);
            }
        }
    }
    
    protected String storeCounts(StoreForTests store1, StoreForTests... stores)
            throws StoreException
    {
        StringBuilder sb = new StringBuilder();
        sb.append(store1.getEntryStore().getEntryCount());
        long ts = ((AdminStorableStore) store1.getEntryStore()).getTombstoneCount(StoreOperationSource.ADMIN_TOOL, 5000L);
        if (ts > 0) {
            sb.append('(').append(ts).append(')');
        }
        for (StoreForTests store : stores) {
            sb.append('/').append(store.getEntryStore().getEntryCount());
            ts = ((AdminStorableStore)store.getEntryStore()).getTombstoneCount(StoreOperationSource.ADMIN_TOOL, 5000L);
            if (ts > 0) {
                sb.append('(').append(ts).append(')');
            }
        }
        return sb.toString();
    }
}
