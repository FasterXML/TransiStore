package com.fasterxml.transistore.dw;

import io.dropwizard.cli.ServerCommand;
import io.dropwizard.Application;
import io.dropwizard.setup.Environment;
import net.sourceforge.argparse4j.inf.Namespace;

public class CustomServerCommand extends ServerCommand<BasicTSServiceConfigForDW>
{
    public CustomServerCommand(Application<BasicTSServiceConfigForDW> service) {
        super(service);
    }

    @Override
    protected void run(Environment environment,
            Namespace namespace,
            BasicTSServiceConfigForDW configuration) throws Exception
    {
        // Ha! Begone automatic GZIP filter, you fool!
        configuration.overrideGZIPEnabled(false);
        super.run(environment, namespace, configuration);
    }
}
