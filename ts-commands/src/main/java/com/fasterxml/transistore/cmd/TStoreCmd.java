package com.fasterxml.transistore.cmd;

import static io.airlift.command.OptionType.GLOBAL;
import io.airlift.command.Option;

public abstract class TStoreCmd implements Runnable
{
    @Option(type = GLOBAL, name = { "-v", "--verbose"}, description = "Verbose mode")
    public boolean verbose = false;

    @Option(type = GLOBAL, name = { "-c", "--config-file" }, description = "Config file to use",
            arity=1 )
    public String configFile;

    @Option(type = GLOBAL, name = { "-t", "--text"}, description = "Textual output mode (vs JSON)")
    public boolean isTextual = true;

    @Option(type = GLOBAL, name = { "-j", "--json"}, description = "JSON output mode (vs textual)")
    public boolean isJSON = false;
}
