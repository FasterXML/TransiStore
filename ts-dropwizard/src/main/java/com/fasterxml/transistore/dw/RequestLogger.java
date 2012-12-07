package com.fasterxml.transistore.dw;

//import com.yammer.dropwizard.logging.Log;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestLogger
{
    /**
     * If logging using standard logger, logger to use
     */
    private final Logger _requestLog;
    
    public RequestLogger(Class<?> cls) {
        this(cls.getName());
    }

    public RequestLogger(String type)
    {
        type = (type == null) ? "" : type.trim();
        if (type.isEmpty()) {
            _requestLog = null;
        } else {
            _requestLog = LoggerFactory.getLogger(type);
        }
    }

    @SuppressWarnings("unused")
    public void logRequest(String method, String path, Response resp, long nanos)
    {
        if (_requestLog == null) return;

        // print timings with fraction of milliseconds...
        int msec10 = (int) (nanos / (100.0 * 1000.0));
        int fraction = msec10 % 10;
        int msecs = msec10 - fraction;
        int status = (resp == null) ? -1 : resp.getStatus();
        
//        _requestLog.info("{} {} returned {} in {}.{} msec", method, path, status, msecs, fraction);
    }
}
