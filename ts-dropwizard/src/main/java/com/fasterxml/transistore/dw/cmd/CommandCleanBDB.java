package com.fasterxml.transistore.dw.cmd;

import net.sourceforge.argparse4j.inf.Namespace;

import com.fasterxml.clustermate.service.Stores;
import com.fasterxml.storemate.store.AdminStorableStore;
import com.fasterxml.transistore.dw.VagabondDWConfig;

import com.yammer.dropwizard.config.Bootstrap;

/**
 * Command for cleaning up all the entry data from BDB (but not
 * store metadata)
 */
public class CommandCleanBDB extends VCommand<VagabondDWConfig>
{
    public CommandCleanBDB() {
        super("nuke",
                "Command to delete contents of Entity BDB store -- use with care!");
    }

    @Override
    protected void run(Bootstrap<VagabondDWConfig> bootstrap,
            Namespace namespace,
            VagabondDWConfig configuration) throws Exception
    {
        // use bigger cache, 40 megs, since we may need to traverse quite a bit
        Stores<?,?> stores = openReadWriteStores(configuration, 40);
        final AdminStorableStore entries = (AdminStorableStore) stores.getEntryStore();

        int total = 0;
        int count;
        long next = System.currentTimeMillis() + 1000L;

        while ((count = entries.removeEntries(500)) > 0) {
            long now = System.currentTimeMillis();
            total += count;
            if (now >= next) {
                next = now + 5000L;
                System.out.printf("Deleted %d entries...\n", total);
            }
        }
        System.out.printf("DONE: deleted %d entries all in all, closing...\n", total);
        stores.stop();
        System.out.println("Stores succesfully closed.");
    }
}
