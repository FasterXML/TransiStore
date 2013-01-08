package com.fasterxml.transistore.cmd;

import static io.airlift.command.OptionType.GLOBAL;
import io.airlift.command.Option;

public abstract class TStoreCmd implements Runnable
{
    @Option(type = GLOBAL, name = "-v", description = "Verbose mode")
    public boolean verbose;

}
