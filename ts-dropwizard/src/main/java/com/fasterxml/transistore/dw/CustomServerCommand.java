package com.fasterxml.transistore.dw;

import net.sourceforge.argparse4j.inf.Namespace;

import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.cli.ServerCommand;
import com.yammer.dropwizard.config.Environment;

public class CustomServerCommand extends ServerCommand<BasicTSServiceConfigForDW>
{
    public CustomServerCommand(Service<BasicTSServiceConfigForDW> service) {
        super(service);
    }

    @Override
    protected void run(Environment environment, Namespace namespace,
            BasicTSServiceConfigForDW configuration) throws Exception
    {
        // Ha! Begone automatic GZIP filter, you fool!
        configuration.getHttpConfiguration().getGzipConfiguration().setEnabled(false);
        super.run(environment, namespace, configuration);
    }
}
