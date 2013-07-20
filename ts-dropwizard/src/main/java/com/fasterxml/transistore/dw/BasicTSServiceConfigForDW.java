package com.fasterxml.transistore.dw;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.fasterxml.storemate.backend.bdbje.BDBJEBuilder;
import com.fasterxml.transistore.service.cfg.BasicTSServiceConfig;
import com.fasterxml.clustermate.dw.DWConfigBase;
import com.yammer.dropwizard.config.HttpConfiguration;

/**
 * Wrapper class used when embedding configuration to be used for
 * DropWizard-based service
 */
public class BasicTSServiceConfigForDW
    extends DWConfigBase<BasicTSServiceConfig, BasicTSServiceConfigForDW>
{
    /**
     * We will define explicit constructor to define custom overrides
     * to DropWizard default settings; these can then be overridden
     * by JSON config file (and System properties).
     */
    public BasicTSServiceConfigForDW() {
        ts.storeBackendType = BDBJEBuilder.class;
    }

    /**
     * Configuration Object that contains all TS-specific
     * settings (and none of DropWizard standard ones)
     */
    @NotNull
    @Valid
    public BasicTSServiceConfig ts = new BasicTSServiceConfig();

    @Override
    public BasicTSServiceConfig getServiceConfig() {
        return ts;
    }

    /**
     * Need to override this method to ensure that under no circumstances
     * do we let GZIP compression be enabled.
     */
    @Override
    public void setHttpConfiguration(HttpConfiguration config)
    {
        if (config != null) {
            config.getGzipConfiguration().setEnabled(false);
        }
        super.setHttpConfiguration(config);
    }
}
