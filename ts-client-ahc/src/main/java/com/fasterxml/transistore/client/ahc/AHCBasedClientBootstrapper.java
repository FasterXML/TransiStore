package com.fasterxml.transistore.client.ahc;

import com.fasterxml.transistore.client.BasicTSClientBootstrapper;
import com.fasterxml.transistore.client.BasicTSClientConfig;

/**
 * Convenience class that can be directly instantiated to use
 * AHC-based client for access.
 */
public class AHCBasedClientBootstrapper
	extends BasicTSClientBootstrapper
{
	public AHCBasedClientBootstrapper(BasicTSClientConfig config)
	{
		super(config, new AHCBasedClient(config));
	}
}
