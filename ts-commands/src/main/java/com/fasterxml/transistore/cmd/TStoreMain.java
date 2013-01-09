package com.fasterxml.transistore.cmd;

import io.airlift.command.Cli;
import io.airlift.command.Cli.CliBuilder;
import io.airlift.command.Help;

public class TStoreMain
{
    public static void main(String[] args)
    {
        CliBuilder<Runnable> builder = Cli.<Runnable>builder("git")
                .withDescription("Main tstore command for listing, copying and removing files")
                .withDefaultCommand(Help.class)
                .withCommands(Help.class, ListCmd.class);

        // If we wanted something like "git [options] remote add [options]", we'd add:
        /*
        builder.withGroup("list")
                .withDescription("List stored entries in specified partition")
                .withDefaultCommand(RemoteShow.class)
                .withCommands(RemoteShow.class, RemoteAdd.class);
                */

        Cli<Runnable> gitParser = builder.build();

        gitParser.parse(args).run();
    }
}
