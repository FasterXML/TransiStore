package com.fasterxml.transistore.dw;

import java.io.IOException;

import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.servlet.ServletServiceRequest;
import com.fasterxml.clustermate.service.servlet.ServletServiceResponse;
import com.fasterxml.clustermate.service.servlet.StoreEntryServlet;
import com.fasterxml.clustermate.service.store.StoreHandler;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.storemate.store.util.OperationDiagnostics;
import com.fasterxml.storemate.store.util.TotalTime;
import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.basic.BasicTSListItem;
import com.fasterxml.transistore.service.cfg.BasicTSServiceConfig;

@SuppressWarnings("serial")
public class BasicTSStoreEntryServlet extends StoreEntryServlet<BasicTSKey, StoredEntry<BasicTSKey>>
{
    protected final boolean _printTimings;
    
    public BasicTSStoreEntryServlet(SharedServiceStuff stuff,
            ClusterViewByServer cluster,
            StoreHandler<BasicTSKey, StoredEntry<BasicTSKey>,BasicTSListItem> storeHandler)
    {
        super(stuff, cluster, storeHandler);
        BasicTSServiceConfig config = stuff.getServiceConfig();
        _printTimings = config.printTimings;
    }

    /*
    /**********************************************************************
    /* Entry point overrides for possible logging etc
    /**********************************************************************
     */

    @Override
    public void handleGet(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats) throws IOException
    {
        super.handleGet(request, response, stats);
        if (_printTimings) {
            _printTiming("GET", request, response, stats);
        }
    }

    @Override
    public void handleHead(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats) throws IOException
    {
        super.handleHead(request, response, stats);
        if (_printTimings) {
            _printTiming("HEAD", request, response, stats);
        }
    }
    
    @Override
    public void handlePut(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats) throws IOException
    {
        super.handlePut(request, response, stats);
        if (_printTimings) {
            _printTiming("PUT", request, response, stats);
        }
    }

    @Override
    public void handleDelete(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats) throws IOException
    {
        super.handleDelete(request, response, stats);
        if (_printTimings) {
            _printTiming("DELETE", request, response, stats);
        }
    }

    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */

    protected void _printTiming(String verb,
            ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats)
    {
        if (stats == null) {
            System.out.printf("%s: no-stats\n", verb);
            return;
        }
        System.out.printf("TIMING:%s -> DB=%s, File=%s, Req/Resp=%.2f, TOTAL=%.2f msec; %d/%d bytes r/w\n",
                verb,
                _time(stats.getDbAccess()),
                _time(stats.getFileAccess()),
                (stats.getContentCopyNanos()>>10) / 1000.0,
                (stats.getNanosSpent() >> 10) / 1000.0,
                request.getBytesRead(), response.getBytesWritten()
                );
    }

    protected String _time(TotalTime time)
    {
        if (time == null) {
            return "-";
        }
        double msec1 = ((int) time.getTotalTimeWithoutWait()>>10) / 1000.0;
        double msec2 = ((int) time.getTotalTimeWithWait()>>10) / 1000.0;
        return String.format("%.2f(w:%.2f)", msec1, (msec2 - msec1));
    }
}
