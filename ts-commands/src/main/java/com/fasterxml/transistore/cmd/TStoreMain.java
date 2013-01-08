package com.fasterxml.transistore.cmd;

import io.airlift.command.Cli;
import io.airlift.command.Cli.CliBuilder;
import io.airlift.command.Help;

public class TStoreMain
{
    public static void main(String[] args)
    {
        CliBuilder<Runnable> builder = Cli.<Runnable>builder("git")
                .withDescription("the stupid content tracker")
                .withDefaultCommand(Help.class)
                .withCommands(Help.class,
                        ListCmd.class);
        /*
                .withDescription("Main tstore command for listing, copying and removing files")
                .withDefaultCommand(Help.class)
                .withCommands(Help.class, ListCmd.class);
        */

        /*
        builder.withGroup("remote")
                .withDescription("Manage set of tracked repositories")
                .withDefaultCommand(RemoteShow.class)
                .withCommands(RemoteShow.class, RemoteAdd.class);
                */

        Cli<Runnable> gitParser = builder.build();

        gitParser.parse(args).run();
    }
}
