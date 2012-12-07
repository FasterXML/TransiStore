package com.fasterxml.transistore.dw.util;

import java.io.InputStream;
import java.util.*;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.OperationType;
import com.fasterxml.clustermate.service.ServiceRequest;

/**
 * {@link ServiceRequest} implementation used by tests.
 * Does not support addition of duplicate query parameters or
 * headers currently.
 */
public class FakeHttpRequest extends ServiceRequest
{
    private HashMap<String, String> _queryParams;
    private HashMap<String, String> _headers;
    
    public FakeHttpRequest() {
        this("");
    }
    public FakeHttpRequest(String path) {
        this(path, OperationType.GET);
    }
    public FakeHttpRequest(String path, OperationType operation) {
        super(path, true, operation);
    }

    public FakeHttpRequest addQueryParam(String key, String value)
    {
        if (_queryParams == null) {
            _queryParams = new HashMap<String, String>();
        }
        _queryParams.put(key, value);
        return this;
    }

    public FakeHttpRequest addHeader(String key, String value)
    {
        if (_headers == null) {
            _headers = new HashMap<String, String>();
        }
        _headers.put(key, value);
        return this;
    }
    
    @Override
    public String getPath() {
        return "";
    }

    @Override
    public String getQueryParameter(String key) {
        return (_queryParams == null) ? null : _queryParams.get(key);
    }

    @Override
    public String getHeader(String key) {
        return (_headers == null) ? null : _headers.get(key);
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Additional methods for tests
    ///////////////////////////////////////////////////////////////////////
     */

    public FakeHttpRequest setAcceptedCompression(String value) {
        return addHeader(ClusterMateConstants.HTTP_HEADER_ACCEPT_COMPRESSION, value);
    }

    @Override
    public InputStream getNativeInputStream() {
        throw new UnsupportedOperationException();
    }
}
