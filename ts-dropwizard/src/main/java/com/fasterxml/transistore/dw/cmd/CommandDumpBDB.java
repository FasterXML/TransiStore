package com.fasterxml.transistore.dw.cmd;

import java.util.List;

import net.sourceforge.argparse4j.inf.Namespace;

import com.yammer.dropwizard.config.Bootstrap;

import com.fasterxml.clustermate.service.Stores;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.store.AdminStorableStore;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.StoreOperationSource;
import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.dw.BasicTSServiceConfigForDW;

/**
 * Command that can be used to dump first parts of BDB contents
 * for debugging; by default first 100 entries will be output.
 */
public class CommandDumpBDB extends CommandBase<BasicTSServiceConfigForDW>
{
    public CommandDumpBDB() {
        super("dump", "Command to dump contents of Entity BDB store");
    }

    @Override
    protected void run(Bootstrap<BasicTSServiceConfigForDW> bootstrap,
            Namespace namespace,
            BasicTSServiceConfigForDW configuration) throws Exception
    {
        Stores<BasicTSKey, StoredEntry<BasicTSKey>> stores = openReadOnlyStores(configuration);
        AdminStorableStore store = (AdminStorableStore) stores.getEntryStore();
        // true/false -> include deleted?
        List<Storable> entries = store.dumpEntries(StoreOperationSource.ADMIN_TOOL, 5000, false);
        StoredEntryConverter<BasicTSKey, StoredEntry<BasicTSKey>,?> factory = stores.getEntryConverter();
        System.out.println("Key,Path,Size,StoredSize,Compressed,Inlined");
        for (Storable raw : entries) {
            StoredEntry<BasicTSKey> entry = factory.entryFromStorable(raw);
            System.out.print(entry.getKey().toString());
            System.out.print(',');
            // If we want Hash, can/need to access ContentKeyHasher
            if (entry.hasExternalData()) {
                System.out.print(raw.getExternalFilePath());
            }
            System.out.print(',');
            System.out.print(entry.getActualUncompressedLength());
            System.out.print(',');
            System.out.print(entry.getStorageLength());
            System.out.print(',');
            Compression comp = entry.getCompression();
            System.out.print((comp == null) ? "?" : comp.name());
            System.out.print(',');
            System.out.print(entry.hasInlineData());
            System.out.println();
        }
        stores.stop();
    }
}
