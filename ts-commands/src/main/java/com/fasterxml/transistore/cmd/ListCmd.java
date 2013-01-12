package com.fasterxml.transistore.cmd;

import java.util.List;

import com.fasterxml.transistore.client.BasicTSClient;
import com.fasterxml.transistore.client.BasicTSClientConfig;

import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;

@Command(name = "list", description = "Lists files")
public class ListCmd extends TStoreCmd
{
    @Option(name = { "-m", "--max" }, description = "Maximum number of entries to list",
            arity=1 )
    public int maxEntries = Integer.MAX_VALUE;

    @Arguments(description = "Partition (and optional name prefix) to list",
            required=true)
    public List<String> pathInfo;

    @Override
    public void run()
    {
        // start with config file
        SkeletalServiceConfig serviceConfig = getServiceConfig();
        BasicTSClientConfig clientConfig = getClientConfig();

        // but also need at least partition
        if (pathInfo.size() > 2) {
            throw new IllegalArgumentException("Too many arguments: can only use 2 (partition, optional prefix)");
        }
        String partition = pathInfo.get(0);
        String prefix = (pathInfo.size() < 2) ? null : pathInfo.get(1);
        BasicTSClient client = bootstrapClient(clientConfig, serviceConfig);

        System.out.println("Ta-dah! Should just... like, list stuff");
        
        client.stop();
    }
}
