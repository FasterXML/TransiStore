package com.fasterxml.transistore.dw.cmd;

import java.io.*;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.storemate.backend.bdbje.BDBJEBuilder;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.backend.StoreBackend;
import com.fasterxml.storemate.store.file.FileManager;
import com.fasterxml.storemate.store.file.FileManagerConfig;
import com.fasterxml.storemate.store.impl.StorableStoreImpl;

import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.dw.BasicTSServiceConfigForDW;
import com.fasterxml.transistore.service.cfg.BasicTSFileManager;
import com.fasterxml.transistore.service.cfg.BasicTSServiceConfig;
import com.fasterxml.transistore.service.store.BasicTSStores;

import com.yammer.dropwizard.cli.ConfiguredCommand;
import com.yammer.dropwizard.config.Configuration;


// NOTE: can NOT parametrize at this level, due to DropWizard issue #89
public abstract class CommandBase<T extends Configuration> extends ConfiguredCommand<T>
{
    protected final static ObjectMapper _mapper = new ObjectMapper();
    
    public CommandBase(String name, String desc)
    {
        super(name, desc);
//        Log.named(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(Level.INFO);
    }
    
    /*
    /**********************************************************************
    /* Methods for opening BDB store(s)
    /**********************************************************************
     */
    
    protected BasicTSStores openReadOnlyStores(BasicTSServiceConfigForDW configuration)
        throws IOException
    {
        return _openStores(configuration, false);
    }

    protected BasicTSStores openReadWriteStores(BasicTSServiceConfigForDW configuration)
        throws IOException
    {
        return _openStores(configuration, true);
    }

    protected BasicTSStores openReadWriteStores(BasicTSServiceConfigForDW configuration, int cacheMegs)
        throws IOException
    {
        return _openStores(configuration, true);
    }
    
    protected BasicTSStores _openStores(BasicTSServiceConfigForDW configuration,
            boolean writeAccess)
        throws IOException
    {
        BasicTSStores stores = _constructStores(configuration);
        if (writeAccess){
            stores.openIfExists();
        } else {
            stores.openForReading(false);
        }
        return stores;
    }

    protected BasicTSStores _constructStores(BasicTSServiceConfigForDW configuration)
        throws IOException
    {
        TimeMaster tm = TimeMaster.nonTestInstance();
        final BasicTSServiceConfig v = configuration.getServiceConfig();

        FileManager files = new BasicTSFileManager(new FileManagerConfig(v.storeConfig.dataRootForFiles), tm);
        BDBJEBuilder b = new BDBJEBuilder();
        StoreBackend backend = b.with(v.storeConfig)
                .with(_mapper.convertValue(v.storeBackendConfig, b.getConfigClass()))
                .buildCreateAndInit();
        // null -> simple throttler
        StorableStore store = new StorableStoreImpl(v.storeConfig, backend, tm, files, null);
        StoredEntryConverter<?,?,?> conv0 = v.getEntryConverter();
        @SuppressWarnings("unchecked")
        StoredEntryConverter<BasicTSKey, StoredEntry<BasicTSKey>,?> entryConv
            = (StoredEntryConverter<BasicTSKey, StoredEntry<BasicTSKey>,?>) conv0;
        return new BasicTSStores(v, tm, new ObjectMapper(), entryConv, store);
    }

    /*
    /**********************************************************************
    /* I/O helper methods
    /**********************************************************************
     */

    protected static void writeFile(File file, byte[] data) throws IOException {
        writeFile(file, data, 0, data.length);
    }
    
    protected static void writeFile(File file, byte[] data, int offset, int length) throws IOException
    {
        FileOutputStream out = new FileOutputStream(file);
        out.write(data, offset, length);
        out.close();
    }
    
    protected static byte[] readFile(File f) throws IOException
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
