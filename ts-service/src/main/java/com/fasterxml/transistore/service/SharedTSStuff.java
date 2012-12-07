package com.fasterxml.transistore.service;

import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.file.FileManager;

import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.clustermate.api.RequestPathStrategy;
import com.fasterxml.clustermate.service.SharedServiceStuff;

import com.fasterxml.transistore.basic.BasicTSKey;

public abstract class SharedTSStuff extends SharedServiceStuff
{
    protected SharedTSStuff(TimeMaster timeMaster, FileManager fileManager,
            RequestPathStrategy pathStrategy) {
        super(timeMaster, fileManager, pathStrategy);
    }

    @Override
    public abstract EntryKeyConverter<BasicTSKey> getKeyConverter();
}
