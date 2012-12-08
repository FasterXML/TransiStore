package com.fasterxml.transistore.dw;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.skife.config.TimeSpan;

import com.fasterxml.storemate.backend.bdbje.BDBJEBuilder;
import com.fasterxml.storemate.backend.bdbje.BDBJEConfig;
import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.shared.hash.ChecksumUtil;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.backend.StoreBackend;
import com.fasterxml.storemate.store.file.FileManager;
import com.fasterxml.storemate.store.file.FileManagerConfig;
import com.fasterxml.storemate.store.impl.StorableStoreImpl;

import com.fasterxml.clustermate.api.KeySpace;
import com.fasterxml.clustermate.api.NodeDefinition;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cfg.ClusterConfig;
import com.fasterxml.clustermate.service.cfg.NodeConfig;
import com.fasterxml.clustermate.service.cluster.ActiveNodeState;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServerImpl;
import com.fasterxml.clustermate.service.store.StoredEntry;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.basic.BasicTSKeyConverter;
import com.fasterxml.transistore.dw.util.FakeHttpResponse;
import com.fasterxml.transistore.dw.util.StoreResourceForTests;
import com.fasterxml.transistore.service.BasicTSEntryConverter;
import com.fasterxml.transistore.service.SharedTSStuffImpl;
import com.fasterxml.transistore.service.cfg.BasicTSFileManager;
import com.fasterxml.transistore.service.cfg.BasicTSServiceConfig;
import com.fasterxml.transistore.service.store.BasicTSStoreHandler;
import com.fasterxml.transistore.service.store.BasicTSStores;


import junit.framework.TestCase;

/**
 * Shared base class for unit tests; contains shared utility methods.
 */
public abstract class JaxrsStoreTestBase extends TestCase
{
    /**
     * Let's use a "non-standard" port number for tests; could use ephemeral one,
     * but may be easier to troubleshoot if we use fixed one.
     * Lucky four sevens...
     */
    protected final static int TEST_PORT = 7777;

    // null -> require client id with key
    protected final BasicTSKeyConverter _keyConverter = BasicTSKeyConverter.defaultInstance();

    protected final BasicTSEntryConverter _entryConverter = new BasicTSEntryConverter(_keyConverter);

    /*
    /**********************************************************************
    /* Configuration setting helpers
    /**********************************************************************
     */

    protected BasicTSServiceConfig createSimpleTestConfig(String testSuffix, boolean cleanUp)
        throws IOException
    {
        // BDB and file store settings:
        File testRoot = getTestScratchDir(testSuffix, cleanUp);
        BasicTSServiceConfig config = new BasicTSServiceConfig();
        config.metadataDirectory = new File(testRoot, "bdb-basictest");
        config.storeConfig.dataRootForFiles = new File(testRoot, "files");
        // shorten sync grace period to 5 seconds for tests:
        config.cfgSyncGracePeriod = new TimeSpan("5s");
        return config;
    }

    protected BasicTSServiceConfig createSingleNodeConfig(String testSuffix,
            boolean cleanUp, int port)
        throws IOException
    {
        BasicTSServiceConfig config = createSimpleTestConfig(testSuffix, cleanUp);

        ClusterConfig cluster = config.cluster;
        cluster.clusterKeyspaceSize = 360;
        ArrayList<NodeConfig> nodes = new ArrayList<NodeConfig>();
        nodes.add(new NodeConfig("localhost:"+TEST_PORT, 0, 360));
        // Cluster config? Set keyspace size, but nothing else yet
        cluster.clusterNodes = nodes;
        return config;
    }

    /*
    /**********************************************************************
    /* Store creation
    /**********************************************************************
     */

    protected StoreResourceForTests<BasicTSKey, StoredEntry<BasicTSKey>>
    createResource(String testSuffix, TimeMaster timeMaster,
            boolean cleanUp)
        throws IOException
    {
        BasicTSServiceConfig config = createSimpleTestConfig(testSuffix, cleanUp);

        File fileDir = config.storeConfig.dataRootForFiles;
        FileManager files = new BasicTSFileManager(new FileManagerConfig(fileDir), timeMaster);

        BDBJEConfig bdbConfig = new BDBJEConfig();
        bdbConfig.dataRoot = new File(fileDir.getParent(), "bdb-storemate");
        BDBJEBuilder b = new BDBJEBuilder(config.storeConfig, bdbConfig);
        StoreBackend backend = b.buildCreateAndInit();
        StorableStore store = new StorableStoreImpl(config.storeConfig, backend, timeMaster, files);
        SharedTSStuffImpl stuff = new SharedTSStuffImpl(config, timeMaster,
                _entryConverter, files);
        BasicTSStores stores = new BasicTSStores(config, timeMaster, stuff.jsonMapper(),
                _entryConverter, store, config.metadataDirectory);
        // important: configure to reduce log noise:
        stuff.markAsTest();
        stores.initAndOpen(false);
        return new StoreResourceForTests<BasicTSKey, StoredEntry<BasicTSKey>>(clusterViewForTesting(stuff, stores),
                        new BasicTSStoreHandler(stuff, stores), stuff);
    }

    /**
     * Bit unclean, but tests need to be able to create a light-weight instance
     * of cluster view.
     */
    protected ClusterViewByServer clusterViewForTesting(SharedServiceStuff stuff,
            BasicTSStores stores)
    {
        KeySpace keyspace = new KeySpace(360);
        NodeDefinition localDef = new NodeDefinition(new IpAndPort("localhost:9999"), 1,
                keyspace.fullRange(), keyspace.fullRange());
        ActiveNodeState localState = new ActiveNodeState(localDef, 0L);
        return new ClusterViewByServerImpl<BasicTSKey, StoredEntry<BasicTSKey>>(stuff, stores, keyspace,
                localState,
                Collections.<IpAndPort,ActiveNodeState>emptyMap(),
                0L);
    }

    /*
    /**********************************************************************
    /* Other factory methods
    /**********************************************************************
     */
    
    protected BasicTSKey contentKey(String path) {
        return _keyConverter.construct(path);
    }

    protected BasicTSKey contentKey(String partitionId, String path) {
        return _keyConverter.construct(partitionId, path);
    }

    protected StoredEntry<BasicTSKey> rawToEntry(Storable raw) {
        if (raw == null) {
    		    return null;
        }
        return _entryConverter.entryFromStorable(raw);
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
        assertTrue(response.hasStreamingContent());
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

    protected String biggerSomewhatCompressibleData(int size)
    {
        StringBuilder sb = new StringBuilder(size + 100);
        final Random rnd = new Random(123);
        while (sb.length() < size) {
            int val = rnd.nextInt();
            switch (val % 5) {
            case 0:
                sb.append('X');
                break;
            case 1:
                sb.append(": ").append(sb.length());
                break;
            case 2:
                sb.append('\n');
                break;
            case 3:
                sb.append((char) (33 + val & 0x3f));
                break;
            default:
                sb.append("/").append(Integer.toHexString(sb.length()));
                break;
            }
        }
        return sb.toString();
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

    /*
    /**********************************************************************
    /* Test methods: message validation
    /**********************************************************************
     */

    protected void verifyException(Exception e, String expected) {
        verifyMessage(expected, e.getMessage());
    }
    
    protected void verifyMessage(String expectedPiece, String actual) {
        if (actual == null || actual.toLowerCase().indexOf(expectedPiece.toLowerCase()) < 0) {
            fail("Expected message that contains phrase '"+expectedPiece+"'; instead got: '"
                    +actual+"'");
        }
    }
	
    /*
    /**********************************************************************
    /* Log setup
    /**********************************************************************
     */

    /**
     * Method to be called before tests, to ensure log4j does not whine.
     */
    protected void initTestLogging()
    {
        // changed for DW 6.0... how to?
//        Log.named(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(Level.WARN);
    }

    /*
    /**********************************************************************
    /* Checksum calculation
    /**********************************************************************
     */

    protected int calcChecksum(byte[] data) {
        return ChecksumUtil.calcChecksum(data);
    }

    protected byte[] gzip(byte[] input) throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream(input.length / 4);
        GZIPOutputStream gz = new GZIPOutputStream(out);
        gz.write(input);
        gz.close();
        return out.toByteArray();
    }

    protected byte[] gunzip(byte[] comp) throws IOException {
        return readAll(new GZIPInputStream(new ByteArrayInputStream(comp)));
    }
}
