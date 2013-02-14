package com.fasterxml.transistore.cmd;

import io.airlift.command.Cli;
import io.airlift.command.Cli.CliBuilder;
import io.airlift.command.Help;

public class TStoreMain
{
    public static void main(String[] args) throws Exception
    {
        @SuppressWarnings("unchecked")
        CliBuilder<Runnable> builder = Cli.<Runnable>builder("tstore")
                .withDescription("Main tstore command for listing, copying and removing files")
                .withDefaultCommand(Help.class)
                .withCommands(Help.class,
                        GetCmd.class, CatCmd.class, ListCmd.class, PutCmd.class);

        // If we wanted something like "git [options] remote add [options]", we'd add:
        /*
        builder.withGroup("list")
                .withDescription("List stored entries in specified partition")
                .withDefaultCommand(RemoteShow.class)
                .withCommands(RemoteShow.class, RemoteAdd.class);
                */

        Cli<Runnable> gitParser = builder.build();

        try {
            gitParser.parse(args).run();
        } catch (IllegalArgumentException e) {
            fail(e);
        } catch (IllegalStateException e) {
            fail(e);
        } catch (RuntimeException e) {
            fail(e);
        }
    }
    
    
    private static void fail(Exception e)
    {
        String msg = e.getMessage();
        if (msg == null || msg.isEmpty()) {
            msg = "(" + e.getClass().getName() +")";
        }
        System.err.println("Failure: "+msg);
        System.exit(1);
    }
}
