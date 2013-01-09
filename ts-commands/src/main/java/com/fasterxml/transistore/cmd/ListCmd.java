package com.fasterxml.transistore.cmd;

import static io.airlift.command.OptionType.GLOBAL;
import io.airlift.command.Command;
import io.airlift.command.Option;

@Command(name = "list", description = "Lists files")
public class ListCmd extends TStoreCmd
{
    @Option(type = GLOBAL, name = { "-m", "--max" }, description = "Maximum number of entries to list",
            arity=1 )
    public int maxEntries = Integer.MAX_VALUE;
    
    @Override
    public void run() {
        System.out.println("Do LIST goddamit!");
    }
}
