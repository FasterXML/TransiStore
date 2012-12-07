package com.fasterxml.transistore.service.cfg;

import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.file.FileManager;
import com.fasterxml.storemate.store.file.FileManagerConfig;
import com.fasterxml.storemate.store.file.FilenameConverter;

import com.fasterxml.transistore.basic.BasicTSKeyConverter;

public class BasicTSFileManager extends FileManager
{
    public BasicTSFileManager(FileManagerConfig config, TimeMaster timeMaster)
    {
        this(config, timeMaster,
                new BasicTSFilenameConverter(BasicTSKeyConverter.defaultInstance()));
    }

    public BasicTSFileManager(FileManagerConfig config, TimeMaster timeMaster,
            FilenameConverter conv)
    {
        super(config, timeMaster, conv);
    }
}
