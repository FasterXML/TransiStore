package com.fasterxml.transistore.client.jdk;

import com.fasterxml.transistore.client.BasicTSClientBootstrapper;
import com.fasterxml.transistore.client.BasicTSClientConfig;

/**
 * Convenience class that can be directly instantiated to use
 * AHC-based client for access.
 */
public class JDKBasedClientBootstrapper
	extends BasicTSClientBootstrapper
{
	public JDKBasedClientBootstrapper(BasicTSClientConfig config)
	{
		super(config, new JDKBasedClient(config));
	}
}
