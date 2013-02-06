package com.fasterxml.transistore.cmd;

import java.io.*;
import java.util.*;

import com.fasterxml.jackson.core.JsonGenerator;

import io.airlift.command.Arguments;
import io.airlift.command.Command;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.client.BasicTSClient;
import com.fasterxml.transistore.client.BasicTSClientConfig;

@Command(name = "get", description = "GET file(s) from TStore into local file system")
public class GetCmd extends TStoreCmdBase
{
    @Arguments(title="arguments",
            description = "Server entries to get (all but last argument); local directory to save them under (last)"
            ,usage="[server-prefix-1] ... [server-prefix-N] [target directory]"
            ,required=true)
    public List<String> arguments;

    @Override
    public void run()
    {
        SkeletalServiceConfig serviceConfig = getServiceConfig();
        BasicTSClientConfig clientConfig = getClientConfig();

        if (arguments == null || arguments.size() < 2) {
            throw new IllegalArgumentException("Nothing to GET");
        }
        BasicTSClient client = bootstrapClient(clientConfig, serviceConfig);

        // Let's actually verify that the last argument is an existing directory, to avoid
        // problems with missing/mixed arguments
        String dirName = arguments.get(arguments.size()  - 1);
        File target = new File(dirName);
        if (!target.exists() || !target.isDirectory()) {
            throw new IllegalArgumentException("No directory with name '"+target.getAbsolutePath()+"'");
        }

        // and then verify that all server sources are valid paths as well
        List<BasicTSKey> prefixes = new ArrayList<BasicTSKey>();
        for (int i = 0, end = arguments.size()-1; i < end; ++i) {
            String str = arguments.get(i);
            try {
                prefixes.add(contentKey(str));
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid server entry reference ("+i+"): "+e.getMessage());
            }
        }
        
        /*
        try {
            int fileCount = _readStuff(client, target, jgen);
            if (jgen == null) {
                System.out.printf("COMPLETE: uploaded %s files\n", fileCount);
            } else {
                jgen.writeEndArray();
            }
        } catch (Exception e) {
            System.err.println("ERROR: ("+e.getClass().getName()+"): "+e.getMessage());
            if (e instanceof RuntimeException) {
                e.printStackTrace(System.err);
            }
            System.exit(1);
        }
        */
        
        
        client.stop();
    }
}
