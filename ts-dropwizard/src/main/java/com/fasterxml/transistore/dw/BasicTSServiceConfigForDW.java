package com.fasterxml.transistore.dw;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.fasterxml.storemate.backend.bdbje.BDBJEBuilder;
import com.fasterxml.transistore.service.cfg.BasicTSServiceConfig;
import com.fasterxml.clustermate.dw.DWConfigBase;

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
        v.storeBackendType = BDBJEBuilder.class;
    }

    /**
     * Configuration Object that contains all Vagabond-specific
     * settings (and none of DropWizard standard ones)
     */
    @NotNull
    @Valid
    public BasicTSServiceConfig v = new BasicTSServiceConfig();

    @Override
    public BasicTSServiceConfig getServiceConfig() {
        return v;
    }
}
