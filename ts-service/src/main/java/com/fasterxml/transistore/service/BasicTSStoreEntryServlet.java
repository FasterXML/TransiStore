package com.fasterxml.transistore.service;

import java.io.IOException;

import com.fasterxml.storemate.store.util.OperationDiagnostics;
import com.fasterxml.storemate.store.util.TotalTime;
import com.fasterxml.storemate.store.util.TotalTimeAndBytes;

import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.store.StoreHandler;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.servlet.ServletServiceRequest;
import com.fasterxml.clustermate.servlet.ServletServiceResponse;
import com.fasterxml.clustermate.servlet.StoreEntryServlet;

import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.service.cfg.BasicTSServiceConfig;

@SuppressWarnings("serial")
public class BasicTSStoreEntryServlet
    extends StoreEntryServlet<BasicTSKey, StoredEntry<BasicTSKey>>
{
//    private final Logger LOG = LoggerFactory.getLogger("TIMING");

    protected final boolean _printTimings;

    public BasicTSStoreEntryServlet(SharedServiceStuff stuff,
            ClusterViewByServer cluster,
            StoreHandler<BasicTSKey, StoredEntry<BasicTSKey>,?> storeHandler)
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
            System.out.printf("PERF/%s -> NO-STATS", verb);
            return;
        }
        String msg = String.format("PERF/%s -> DB=%s, File=%s, Req/Resp=%.2f, TOTAL=%.2f msec; %d/%d bytes r/w",
                verb,
                _time(stats.getDbAccess()),
                _time(stats.getFileAccess()),
                (stats.getRequestResponseTotal()>>10) / 1000.0,
                (stats.getNanosSpent() >> 10) / 1000.0,
                request.getBytesRead(), response.getBytesWritten()
                );
        LOG.info(msg);
    }

    protected String _time(TotalTime time)
    {
        if (time == null) {
            return "-";
        }
        long nanos1 = time.getTotalTimeWithoutWait();
        int usecs1 = (int) (nanos1 >> 10);
        long nanos2 = time.getTotalTimeWithWait() - nanos1;
        int usecs2;
        if (nanos2 < 0L) { // shouldn't happen but...
            usecs2 = -1;
        } else {
            usecs2 = (int) (nanos2 >> 10);
            // also -- truncate 0.1 msecs into 0, to reduce noise
            if (usecs2 < 100) {
                usecs2 = 0;
            }
        }
        
        double msec1 = usecs1 / 1000.0;
        double msec2 = usecs2 / 1000.0;
        if (time instanceof TotalTimeAndBytes) {
            TotalTimeAndBytes timeB = (TotalTimeAndBytes) time;
            long bytes = timeB.getBytes();
            return String.format("%.2f(w:%.2f)/%dB", msec1, msec2, bytes);
        }
        return String.format("%.2f(w:%.2f)", msec1, msec2);
    }
}
